{-# LANGUAGE DeriveGeneric #-}

module Network.Episodict.Node where

import Data.Maybe (catMaybes)
import Control.Concurrent
import qualified Data.Map.Strict as M
import Control.Monad (forever, mapM_)
import Control.Concurrent (ThreadId, forkIO)
import Control.Concurrent.STM (TQueue, TMVar, TVar, writeTVar, readTVarIO, retry, modifyTVar, readTVar, newTVarIO, newTQueueIO, newEmptyTMVarIO, atomically, putTMVar, writeTQueue, tryReadTQueue)
import Network.Socket hiding (recv)

import Network.Episodict.Connection
import Network.Episodict.Message

data Node a b = Node {
  nId :: String,
  peers :: TVar (M.Map String (Connection a b)),
  view :: TVar (M.Map a (ViewValue b)),
  clock :: TVar Integer,
  nodeTimes :: TVar (M.Map String Integer),
  myTID :: TMVar ThreadId
}

-- R1. We assume that there is at most one occurrence of the operation
-- INSERT(x) for each element x, so that once an element has been deleted
-- from the set, it can never again be reinserted
-- (Hrm. I don't know about this. Not sure if there's any reason I can't support multi-insert w/o delete,
-- even if it makes some of the logic more complicated)
-- TODO: Do some insert mandatory dedupe thing later.
insert :: (Ord a) => Node a b -> a -> b -> IO ()
insert n k v = atomically $ do
  -- t[i] := clock[i]
  theTime <- getClock n
  -- cre[x] := i, T[x] := t[i](i)
  let viewValue = ViewValue (nId n) theTime v
  -- V[i] := V U {x}
  modifyTVar (view n) (M.insert k viewValue)
  -- TODO: auto-send?

-- R2. DELETE(x) is only legal at node j if x is currently in j's view
delete :: (Ord a) => Node a b -> a -> IO ()
delete n k = atomically $ do
  viewMap <- readTVar $ view n
  case (M.lookup k viewMap) of
    Just _ -> do
      modifyTVar (view n) (M.delete k)
    Nothing -> return () -- Throw error?
  -- TODO: auto-send?

list :: (Ord a) => Node a b -> IO (M.Map a (ViewValue b))
list n = atomically $ readTVar (view n)

newNode :: (Show a, Show b, Ord a) => String -> IO (Node a b)
newNode name = do
  cl <- newTVarIO 0
  p <- newTVarIO M.empty
  v <- newTVarIO M.empty
  t <- newTVarIO $ M.fromList [(name, 0)]
  tid <- newEmptyTMVarIO
  let n = Node name p v cl t tid
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
    localView <- readTVar $ view n
    localTimeVector <- readTVar $ nodeTimes n
    let otherView = viewMap msg
    let comboView = M.union localView otherView
    let remoteTimeVector = t msg
    let theNewView = M.foldrWithKey (\k cVV newView -> case ((not $ shouldDelete localView localTimeVector k cVV) && (not $ shouldDelete otherView remoteTimeVector k cVV)) of
                                        True -> (M.insert k cVV newView)
                                        False -> newView
                   ) M.empty comboView
    let theNewTimes = M.foldrWithKey (\k kTime newTimes -> case (M.lookup k newTimes) of
          Nothing -> M.insert k kTime newTimes
          Just otherTime -> case (kTime < otherTime) of
            True -> M.insert k otherTime newTimes
            False -> M.insert k kTime newTimes
                                     ) remoteTimeVector localTimeVector
    writeTVar (view n) theNewView
    writeTVar (nodeTimes n) theNewTimes
  return n

-- del(V, t, x) iff [x nin V and T[x] <= t(cre[x])]
shouldDelete :: (Ord a) => (M.Map a (ViewValue b)) -> (M.Map String Integer) ->  a -> (ViewValue b) -> Bool
shouldDelete localView timeVector k vV = case (M.lookup k localView) of
    Just _ -> False
    Nothing -> case (M.lookup (creator vV) timeVector) of
      Nothing -> False -- Uhhhhhh...
      Just creatorTime -> ((time vV) <= creatorTime)

createMessage :: (Ord a) => Node a b -> IO (Message a b)
createMessage n = atomically $ do
  v <- readTVar (view n)
  times <- readTVar $ nodeTimes n
  cl <- getClock n
  return $ Message v (M.insert (nId n) cl times) (nId n)

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

getClock n = do
  modifyTVar (clock n) (+1)
  newClock <- readTVar $ clock n
  modifyTVar (nodeTimes n) (M.insert (nId n) newClock)
  return newClock

testLocal :: IO ()
testLocal = do
  node1 <- newNode "node1"
  node2 <- newNode "node2"
  node3 <- newNode "node3"
  connectNodes node1 node2
  connectNodes node2 node3
  threadDelay 1000
  Network.Episodict.Node.insert node1 "a" "b"
  Network.Episodict.Node.send node1
  threadDelay 1000
  Network.Episodict.Node.insert node2 "c" "3"
  Network.Episodict.Node.send node2
  threadDelay 1000
  Network.Episodict.Node.delete node1 "a"
  Network.Episodict.Node.send node1
  threadDelay 1000
  Network.Episodict.Node.delete node3 "c"
  Network.Episodict.Node.send node3
  threadDelay 1000
  Network.Episodict.Node.send node2
  threadDelay 1000
  Network.Episodict.Node.insert node3 "d" "4"
  Network.Episodict.Node.send node3
  threadDelay 1000
  Network.Episodict.Node.send node2
  threadDelay 1000
  l1 <- list node1
  print "list of node 1"
  print l1
  times <- readTVarIO $ nodeTimes node1
  print times
  l2 <- list node2
  print "list of node 2"
  print l2
  times <- readTVarIO $ nodeTimes node2
  print times
  l3 <- list node3
  print "list of node 3"
  print l3
  times <- readTVarIO $ nodeTimes node3
  print times

testSocket :: IO ()
testSocket = do
  node1 <- newNode "node1" :: IO (Node String String)
  node2 <- newNode "node2" :: IO (Node String String)
  sock <- newSocket 9999
  sock2 <- newSocket 0
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
