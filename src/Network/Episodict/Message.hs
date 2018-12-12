{-# LANGUAGE DeriveGeneric #-}

module Network.Episodict.Message where

import Data.Binary
import GHC.Generics (Generic)
import qualified Data.Map.Strict as M

data Message a b = Message {
  viewMap :: (M.Map a (ViewValue b)),
  t :: (M.Map String Integer),
  nodeId :: String
} deriving (Show, Generic)

instance (Binary a, Binary b) => Binary (Message a b)

data ViewValue b = ViewValue {
  creator :: String,
  time :: Integer,
  value :: b
} deriving (Show, Generic)

instance (Binary b) => Binary (ViewValue b)
