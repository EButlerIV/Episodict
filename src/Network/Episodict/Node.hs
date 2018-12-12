{-# LANGUAGE DeriveGeneric #-}

module Network.Episodict.Node where

import Data.Maybe (catMaybes)
import Control.Concurrent
import qualified Data.Map.Strict as M
import Control.Monad (forever, mapM_)
import Control.Concurrent (ThreadId, forkIO)
import Control.Concurrent.STM (TQueue, TMVar, TVar, retry, modifyTVar, readTVar, newTVarIO, newTQueueIO, newEmptyTMVarIO, atomically, putTMVar, writeTQueue, tryReadTQueue)
import Network.Socket hiding (recv)

import Network.Episodict.Connection
import Network.Episodict.Message

data Node a b = Node {
  nId :: String,
  peers :: TVar (M.Map String (Connection a b)),
  inserted :: TVar (M.Map a b),
  deleted :: TVar (M.Map a ()),
  clock :: TVar Integer,
  myTID :: TMVar ThreadId
}

insert :: (Ord a) => Node a b -> a -> b -> IO ()
insert n k v = atomically $ do
  modifyTVar (inserted n) (M.insert k v)
  modifyTVar (clock n) (+ 1)
  return ()
  -- TODO: auto-send?
  -- return $ n { inserted = newInserted, clock = newClock }

delete :: (Ord a) => Node a b -> a -> IO ()
delete n k = atomically $ do
  modifyTVar (deleted n) (M.insert k ())
  return ()
  -- TODO: auto-send?
  -- return $ n { deleted = newDeleted }

list :: (Ord a) => Node a b -> IO (M.Map a b)
list n = atomically $ do
  ins <- readTVar (inserted n)
  del <- readTVar (deleted n)
  return $ M.difference ins del

newNode :: (Show a, Show b, Ord a) => String -> IO (Node a b)
newNode name = do
  ins <- newTVarIO M.empty
  del <- newTVarIO M.empty
  cl <- newTVarIO 0
  p <- newTVarIO M.empty
  tid <- newEmptyTMVarIO
  let n = Node name p ins del cl tid
  nodeReceive n
  return n

nodeReceive :: (Ord a) => Node a b -> IO ()
nodeReceive n = do
    tid <- forkIO t
    atomically $ putTMVar (myTID n) tid
  where t = forever $  do
              msgs <- atomically $ do
                prs <- readTVar $ peers n
                msgs <- mapM (\(k, conn) -> tryReadTQueue $ rx conn) (M.toList prs)
                let arrivedMsgs = catMaybes msgs
                case (length arrivedMsgs == 0) of
                  True -> retry
                  False -> return arrivedMsgs
              mapM_ (receive n) msgs


send :: (Show a, Show b, Ord a) => Node a b -> IO ()
send n = do
  peerConnections <- atomically $ readTVar $ peers n
  message <- createMessage n
  mapM_ (sendMessage message) (Prelude.map snd $ M.toList peerConnections)

receive :: (Ord a) => Node a b -> Message a b -> IO (Node a b)
receive n msg = do
  atomically $ do
    modifyTVar (inserted n) (M.union (insertedMap msg))
    modifyTVar (deleted n) (M.union (deletedMap msg))
  -- TODO: actually reconcile the dicts
  return n

createMessage :: (Ord a) => Node a b -> IO (Message a b)
createMessage n = atomically $ do
  ins <- readTVar (inserted n)
  del <- readTVar (deleted n)
  cl <- readTVar (clock n)
  return $ Message ins del (cl) (nId n)

sendMessage :: (Show a, Show b) => Message a b -> Connection a b -> IO ()
sendMessage message conn = do
  print $ (nodeId message) ++ " sent message"
  atomically $ writeTQueue (tx conn) message

connectNodes :: Node a b -> Node a b -> IO ()
connectNodes n1 n2 = do
  (c1, c2) <- newLocalConnectionPair
  atomically $ do
    modifyTVar (peers n1) (M.insert (nId n2) c1)
    modifyTVar (peers n2) (M.insert (nId n1) c2)

testLocal :: IO ()
testLocal = do
  node1 <- newNode "node1"
  node2 <- newNode "node2"
  connectNodes node1 node2
  Network.Episodict.Node.insert node1 "a" "b"
  Network.Episodict.Node.send node1
  Network.Episodict.Node.insert node2 "c" "3"
  Network.Episodict.Node.send node2
  Network.Episodict.Node.delete node1 "a"
  Network.Episodict.Node.send node1
  threadDelay 1000
  l1 <- list node1
  print "list of node 1"
  print l1
  l2 <- list node2
  print "list of node 2"
  print l2

testSocket :: IO ()
testSocket = do
  node1 <- newNode "node1" :: IO (Node String String)
  node2 <- newNode "node2" :: IO (Node String String)
  sock <- newSocket 9999
  sock2 <- newSocket 10000
  listen sock 1
  connect sock2 (SockAddrInet 9999 (tupleToHostAddress (127,0,0,1)))
  (s, _) <- accept sock
  sConn <- newSocketConnection s :: IO (Connection String String)
  sConn2 <- newSocketConnection sock2 :: IO (Connection String String)
  atomically $ modifyTVar (peers node1) (M.insert (nId node2) sConn)
  atomically $ modifyTVar (peers node2) (M.insert (nId node1) sConn2)
  Network.Episodict.Node.insert node1 "a" "b"
  Network.Episodict.Node.send node1
  Network.Episodict.Node.insert node2 "c" "3"
  Network.Episodict.Node.send node2
  Network.Episodict.Node.delete node1 "a"
  Network.Episodict.Node.send node1
  threadDelay 100000
  l1 <- list node1
  print "list of node 1"
  print l1
  l2 <- list node2
  print "list of node 2"
  print l2
  return ()
