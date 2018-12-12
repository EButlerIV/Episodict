{-# LANGUAGE DeriveGeneric #-}

module Network.Episodict.Message where

import Data.Binary
import GHC.Generics (Generic)
import qualified Data.Map.Strict as M

data Message a b = Message {
  insertedMap :: M.Map a b,
  deletedMap :: M.Map a (),
  t :: Integer,
  nodeId :: String
} deriving (Show, Generic)

instance (Binary a, Binary b) => Binary (Message a b)
