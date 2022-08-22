{-# LANGUAGE NumericUnderscores #-}

module Main (main) where

import Data.CRDT.EventFold (UpdateResult(UpdateResult, urEventFold), new)
import Data.CRDT.EventFold.Monad (MonadUpdateEF(event, participate),
  runEventFoldT)
import qualified Data.Binary as Binary
import qualified Data.ByteString.Lazy as BSL


main :: IO ()
main = do
  ((), UpdateResult { urEventFold = ef }) <-
    runEventFoldT 'a' (new () 'a') $ do
      _ <- participate 'b'
      sequence_ $ replicate 1_000 (event ())

  let bytes = Binary.encode ef
  print ("Encoding size", BSL.length bytes)

  let ef2 = Binary.decode bytes
  print ("round trip equality", ef2 == ef)
  print ("decoded value", ef2)


