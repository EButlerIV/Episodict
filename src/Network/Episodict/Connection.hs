module Network.Episodict.Connection where

import Control.Monad (forever)
import Control.Concurrent (ThreadId, forkIO)
import Control.Concurrent.STM (TQueue, TMVar, newTQueueIO, newEmptyTMVarIO, atomically, putTMVar, writeTQueue, tryReadTQueue)

import System.IO (Handle, IOMode( ReadWriteMode ), hSetBinaryMode)
import Data.Binary
import Network.Socket hiding (recv)
import Network.Socket.ByteString

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL

import Network.Episodict.Message (Message)

data Connection a b = LocalConnection {
  rx :: TQueue (Message a b),
  tx :: TQueue (Message a b)
} | SocketConnection {
  rx :: TQueue (Message a b),
  tx :: TQueue (Message a b),
  connSocket :: Handle, -- Eventually expose separate read/write handles
  listenerTID :: TMVar ThreadId,
  writerTID :: TMVar ThreadId
}

newLocalConnectionPair :: IO ((Connection a b, Connection a b))
newLocalConnectionPair = do
  theTx <- newTQueueIO
  theRx <- newTQueueIO
  return (LocalConnection theTx theRx, LocalConnection theRx theTx)

newSocketConnection :: (Binary a, Binary b, Show a, Show b) => Socket -> IO (Connection a b)
newSocketConnection s = do
  theTx <- newTQueueIO
  theRx <- newTQueueIO
  h <- socketToHandle s ReadWriteMode
  hSetBinaryMode h True
  -- lID <- newSocketListener (s, saddr)
  lIDVar <- newEmptyTMVarIO
  wIDVar <- newEmptyTMVarIO
  newSocketListener $ SocketConnection theTx theRx h lIDVar wIDVar

newSocketListener :: (Binary a, Binary b, Show a, Show b) => Connection a b -> IO (Connection a b) -- Should also specify serialization format!
newSocketListener c = do
    let h = connSocket c
    lId <- forkIO $ reader h
    wId <- forkIO $ writer h
    atomically $ putTMVar (listenerTID c) lId
    atomically $ putTMVar (writerTID c) wId
    return c
  where reader h = forever $ do
          cLengthBS <- (BSL.hGet h 8)
          let cLength = (decode cLengthBS :: Int)
          msgBS <- BSL.hGet h cLength
          let theMsg = defaultDeserialize msgBS
          atomically $ writeTQueue (rx c) theMsg
        writer h = forever $ do
          mOutput <- atomically $ tryReadTQueue $ tx c
          case (mOutput) of
            Nothing -> return ()
            Just x -> BS.hPut h (defaultSerialize x)

defaultSerialize :: (Binary a, Binary b) => Message a b -> BS.ByteString
defaultSerialize msg = BS.concat [contentLength, content]
  where content = BSL.toStrict $ encode msg
        contentLength = BSL.toStrict $ encode $ BS.length content -- TODO: Lock down contentLength representation/size. May be different on different platforms

defaultDeserialize :: (Binary a, Binary b) => BSL.ByteString -> Message a b
defaultDeserialize = decode

newSocket :: PortNumber -> IO (Socket)
newSocket port = do
  sock <- socket AF_INET Stream 0
  setSocketOption sock ReuseAddr 1
  bind sock (SockAddrInet port iNADDR_ANY)
  return sock
