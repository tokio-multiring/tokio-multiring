//! TCP listeners and streams whose I/O goes through a [`Sidecar`].
//!
//! They hold a socket in the mode the rings need and name the operations
//! that make sense on it. Nothing here is a different path to the kernel:
//! every method forwards to the operation of the same name on [`Sidecar`].

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;

use socket2::{Domain, Protocol, SockRef, Socket, Type};

use crate::buffers::FixedBuf;
use crate::ops::{Reclaim, Segment};
use crate::reactor::Sidecar;

/// A TCP socket that listens for connections.
pub struct TcpListener {
    sidecar: Sidecar,
    fd: Arc<OwnedFd>,
}

impl TcpListener {
    /// Listens on `addr`.
    pub fn bind(sidecar: &Sidecar, addr: SocketAddr) -> io::Result<TcpListener> {
        Self::listen(sidecar, addr, false)
    }

    /// Listens on `addr`, which other listeners made this way may share.
    ///
    /// The kernel deals each new connection to one of the listeners on an
    /// address, by a hash of where it comes from. A listener per worker,
    /// each accepted from on its own worker, therefore spreads connections
    /// over the workers without any of them contending for a socket.
    ///
    /// Port 0 picks a free port for this listener alone. To share it, bind
    /// the others to [`local_addr`](Self::local_addr).
    pub fn bind_reuse_port(sidecar: &Sidecar, addr: SocketAddr) -> io::Result<TcpListener> {
        Self::listen(sidecar, addr, true)
    }

    fn listen(sidecar: &Sidecar, addr: SocketAddr, reuse_port: bool) -> io::Result<TcpListener> {
        let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        if reuse_port {
            socket.set_reuse_port(true)?;
        }
        socket.bind(&addr.into())?;
        socket.listen(libc::SOMAXCONN)?;
        Ok(TcpListener {
            sidecar: sidecar.clone(),
            fd: Arc::new(socket.into()),
        })
    }

    /// Takes over a listener made elsewhere, putting it in blocking mode.
    pub fn from_std(sidecar: &Sidecar, listener: std::net::TcpListener) -> io::Result<TcpListener> {
        listener.set_nonblocking(false)?;
        Ok(TcpListener {
            sidecar: sidecar.clone(),
            fd: Arc::new(listener.into()),
        })
    }

    /// The address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket_addr(SockRef::from(&self.fd).local_addr()?)
    }

    /// Waits for a connection and returns it.
    ///
    /// If the future is dropped, a connection that had already been
    /// accepted for it is closed.
    pub fn accept(&self) -> impl Future<Output = io::Result<TcpStream>> + Send + use<> {
        let sidecar = self.sidecar.clone();
        let accepted = sidecar.accept(self.fd.clone());
        async move {
            Ok(TcpStream {
                sidecar,
                fd: Arc::new(accepted.await?),
            })
        }
    }
}

impl AsFd for TcpListener {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

/// A TCP connection. Cheap to clone: the clones are the same connection,
/// which is how one task can send on it while another receives.
#[derive(Clone)]
pub struct TcpStream {
    sidecar: Sidecar,
    fd: Arc<OwnedFd>,
}

impl TcpStream {
    /// Takes over a connection made elsewhere, putting it in blocking mode.
    pub fn from_std(sidecar: &Sidecar, stream: std::net::TcpStream) -> io::Result<TcpStream> {
        stream.set_nonblocking(false)?;
        Ok(TcpStream {
            sidecar: sidecar.clone(),
            fd: Arc::new(stream.into()),
        })
    }

    /// The address of this end of the connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        socket_addr(SockRef::from(&self.fd).local_addr()?)
    }

    /// The address of the other end of the connection.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        socket_addr(SockRef::from(&self.fd).peer_addr()?)
    }

    /// Whether small sends go out at once (`true`) or may wait to be
    /// combined with what follows them.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        SockRef::from(&self.fd).set_tcp_nodelay(nodelay)
    }

    /// See [`Sidecar::recv`].
    pub fn recv(
        &self,
        buf: FixedBuf,
        limit: usize,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        self.sidecar.recv(self.fd.clone(), buf, limit)
    }

    /// See [`Sidecar::recv_vectored`].
    pub fn recv_vectored(
        &self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> impl Future<Output = (io::Result<usize>, Vec<FixedBuf>)> + Send + use<> {
        self.sidecar.recv_vectored(self.fd.clone(), bufs, segments)
    }

    /// See [`Sidecar::send`].
    pub fn send(
        &self,
        buf: FixedBuf,
        start: usize,
    ) -> impl Future<Output = (io::Result<usize>, FixedBuf)> + Send + use<> {
        self.sidecar.send(self.fd.clone(), buf, start)
    }

    /// See [`Sidecar::send_vectored`].
    pub fn send_vectored(
        &self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> impl Future<Output = (io::Result<usize>, Vec<FixedBuf>)> + Send + use<> {
        self.sidecar.send_vectored(self.fd.clone(), bufs, segments)
    }

    /// See [`Sidecar::send_zc_vectored`].
    pub fn send_zc_vectored(
        &self,
        bufs: Vec<FixedBuf>,
        segments: &[Segment],
    ) -> impl Future<Output = (io::Result<usize>, Reclaim)> + Send + use<> {
        self.sidecar
            .send_zc_vectored(self.fd.clone(), bufs, segments)
    }

    /// See [`Sidecar::shutdown`].
    pub fn shutdown(
        &self,
        how: std::net::Shutdown,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        self.sidecar.shutdown(self.fd.clone(), how)
    }
}

impl AsFd for TcpStream {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

fn socket_addr(addr: socket2::SockAddr) -> io::Result<SocketAddr> {
    addr.as_socket().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "the socket is not an internet socket",
        )
    })
}
