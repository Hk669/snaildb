use std::sync::mpsc;
use std::thread;
use std::time::Duration;

// worker manager, manages the mpsc channel across wal, compaction, etc..
pub struct WorkerManager<C> {
    sender: mpsc::Sender<C>,
    _thread_handle: thread::JoinHandle<()>, // thread handle to join the thread
}

impl<C> WorkerManager<C> {
    pub fn send(&self, cmd: C) -> Result<(), mpsc::SendError<C>> {
        self.sender.send(cmd)
    }

    pub fn spawn<F>(handler: F, timeout: Duration) -> Self
    where
        F: FnOnce(mpsc::Receiver<C>, Duration) + Send + 'static, // custom function to handle the messages 
        C: Send + 'static, // command type to send
        {
            let (sender, receiver) = mpsc::channel();
            let handle = thread::spawn(move || {
                handler(receiver, timeout); // call the custom function with the receiver and timeout
            });
            Self { sender, _thread_handle: handle }
        }
}