{-# LANGUAGE NumericUnderscores #-}

module Main (main) where

import Data.CRDT.EventFold (UpdateResult(UpdateResult, urEventFold),
  acknowledge, new)
import Data.CRDT.EventFold.Monad (MonadUpdateEF(event, participate),
  runEventFoldT)


main :: IO ()
main = do
  ((), UpdateResult { urEventFold = ef }) <-
    runEventFoldT 'a' (new () 'a') $ do
      _ <- participate 'b'
      sequence_ $ replicate 500_000 (event ())

  let ur = acknowledge 'b' ef
  print ur


