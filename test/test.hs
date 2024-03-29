{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}

{-# OPTIONS_GHC -Wno-name-shadowing -Wno-incomplete-uni-patterns #-}

{- | crdt-event-fold package tests. -}
module Main (
  main,
) where


import Data.CRDT.EventFold (Event(Output, State, apply),
  EventResult(Pure), UpdateResult(urEventFold, urNeedsPropagation,
  urOutputs), EventFold, acknowledge, allParticipants, bottomEid,
  diffMerge, disassociate, divergent, event, events, fullMerge,
  infimumParticipants, infimumValue, new, participate, projParticipants)
import Data.Maybe (fromJust)
import Prelude (Bool(False, True), Either(Right), Enum(pred, succ),
  Maybe(Just, Nothing), Monoid(mempty), Num(negate), Show(show), ($),
  (.), (<$>), Char, Eq, IO, Int, const, snd)
import Test.Hspec (describe, hspec, it, shouldBe)
import qualified Data.Map as Map
import qualified Data.Set as Set


data Ops
  = Inc
  | Dec
  deriving stock (Eq, Show)

instance Event Char Ops where
  type State Ops = Int
  type Output Ops = Int
  apply Inc state = Pure state (succ state)
  apply Dec state = Pure state (pred state)


main :: IO ()
main = hspec $ do
  describe "EventFold" $
    it "works in a specific contrived scenario" $ do
      {- New crdt with the participant 'a'.  -}
      let a = new 0 'a' :: EventFold Int Char Ops
      -- show a `shouldBe` "EventFold {psOrigin = 0, psInfimum = Infimum {eventId = BottomEid, participants = fromList \"a\", stateValue = 0}, psEvents = fromList [], psUnjoins = fromList []}"
      infimumValue a `shouldBe` 0

      {- 'b' starts participating.  -}
      let (_eid, r) = participate 'a' 'b' a
      let a = urEventFold r
      let b = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = BottomEid, participants = fromList \"a\", stateValue = 0}, psEvents = fromList [(Eid 0 'a',(Identity (Join 'b'),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = BottomEid, participants = fromList \"a\", stateValue = 0}, psEvents = fromList [(Eid 0 'a',(Identity (Join 'b'),fromList \"a\"))]}}"

      let r = acknowledge 'b' b
      let b = urEventFold r
      let Right r = diffMerge 'a' a (fromJust (events 'a' b))
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList []}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList []}}"

      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList []"
      show (divergent b) `shouldBe` "fromList []"

      {- 'a' decrements. -}
      let (o, _, r) = event 'a' Dec a
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList [(Eid 1 'a',(Identity (EventD Dec),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList []}}"
      o `shouldBe` 0
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 0 'a')]"
      show (divergent b) `shouldBe` "fromList []"

      {- 'a' decrements again.  -}
      let (o, _, r) = event 'a' Dec a
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList [(Eid 1 'a',(Identity (EventD Dec),fromList \"a\")),(Eid 2 'a',(Identity (EventD Dec),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList []}}"
      o `shouldBe` negate 1
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 0 'a')]"
      show (divergent b) `shouldBe` "fromList []"

      {- 'b' decrements.  -}
      let (o, _, r) = event 'b' Dec b
      let b = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList [(Eid 1 'a',(Identity (EventD Dec),fromList \"a\")),(Eid 2 'a',(Identity (EventD Dec),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList [(Eid 1 'b',(Identity (EventD Dec),fromList \"b\"))]}}"
      o `shouldBe` 0
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 0 'a')]"
      show (divergent b) `shouldBe` "fromList [('a',Eid 0 'a')]"

      {- | 'b' sends update to 'a'.  -}
      let Right r = diffMerge 'a' a (fromJust (events 'a' b))
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList [(Eid 1 'a',(Identity (EventD Dec),fromList \"a\")),(Eid 1 'b',(Identity (EventD Dec),fromList \"ab\")),(Eid 2 'a',(Identity (EventD Dec),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList [(Eid 1 'b',(Identity (EventD Dec),fromList \"b\"))]}}"
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 1 'b')]"
      show (divergent b) `shouldBe` "fromList [('a',Eid 0 'a')]"

      {- | 'a' sends update to 'b'.  -}
      let Right r = diffMerge 'b' b (fromJust (events 'b' a))
      let b = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 0 'a', participants = fromList \"ab\", stateValue = 0}, psEvents = fromList [(Eid 1 'a',(Identity (EventD Dec),fromList \"a\")),(Eid 1 'b',(Identity (EventD Dec),fromList \"ab\")),(Eid 2 'a',(Identity (EventD Dec),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList []}}"
      show (urOutputs r) `shouldBe` "fromList [(Eid 1 'a',0),(Eid 1 'b',-1),(Eid 2 'a',-2)]"
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 1 'b')]"
      show (divergent b) `shouldBe` "fromList []"

      {- | 'b' sends update to 'a'.  -}
      let Right r = diffMerge 'a' a (fromJust (events 'a' b))
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList []}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList []}}"
      show (urOutputs r) `shouldBe` "fromList [(Eid 1 'a',0),(Eid 1 'b',-1),(Eid 2 'a',-2)]"
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList []"
      show (divergent b) `shouldBe` "fromList []"
      show a `shouldBe` show b

      {- | 'b' sends update to 'a' (again).  -}
      let Right r = diffMerge 'a' a (fromJust (events 'a' b))
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList []}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList []}}"
      show (urOutputs r) `shouldBe` "fromList []"
      urNeedsPropagation r `shouldBe` False
      show (divergent a) `shouldBe` "fromList []"
      show (divergent b) `shouldBe` "fromList []"
      show a `shouldBe` show b

      {- 'a' increments. -}
      let (o, _, r) = event 'a' Inc a
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList []}}"
      o `shouldBe` negate 3
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 2 'a')]"
      show (divergent b) `shouldBe` "fromList []"

      {- 'b' increments. -}
      let (o, _, r) = event 'b' Inc b
      let b = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'b',(Identity (EventD Inc),fromList \"b\"))]}}"
      o `shouldBe` negate 3
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 2 'a')]"
      show (divergent b) `shouldBe` "fromList [('a',Eid 2 'a')]"

      {- 'c' joins on 'a' -}
      let (_eid, r) = participate 'a' 'c' a
      let a = urEventFold r
      let r = acknowledge 'c' a
      let a = urEventFold r
      let c = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'b',(Identity (EventD Inc),fromList \"b\"))]}}"
      -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\"))]}}"
      a `shouldBe` c
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 2 'a')]"
      show (divergent b) `shouldBe` "fromList [('a',Eid 2 'a')]"
      show (divergent c) `shouldBe` "fromList [('b',Eid 2 'a')]"

      {- 'a' increments. -}
      let (o, _, r) = event 'a' Inc a
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'b',(Identity (EventD Inc),fromList \"b\"))]}}"
      -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\"))]}}"
      o `shouldBe` negate 2
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 2 'a'),('c',Eid 4 'a')]"
      show (divergent b) `shouldBe` "fromList [('a',Eid 2 'a')]"
      show (divergent c) `shouldBe` "fromList [('b',Eid 2 'a')]"

      {- 'b' increments. -}
      let (o, _, r) = event 'b' Inc b
      let b = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'b',(Identity (EventD Inc),fromList \"b\")),(Eid 4 'b',(Identity (EventD Inc),fromList \"b\"))]}}"
      -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\"))]}}"
      o `shouldBe` negate 2
      urOutputs r `shouldBe` mempty
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 2 'a'),('c',Eid 4 'a')]"
      show (divergent b) `shouldBe` "fromList [('a',Eid 2 'a')]"
      show (divergent c) `shouldBe` "fromList [('b',Eid 2 'a')]"

      {- | 'b' sends update to 'a'.  -}
      let Right r = diffMerge 'a' a (fromJust (events 'a' b))
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 3 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\")),(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'b',(Identity (EventD Inc),fromList \"b\")),(Eid 4 'b',(Identity (EventD Inc),fromList \"b\"))]}}"
      -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\"))]}}"
      show (urOutputs r) `shouldBe` "fromList []"
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 4 'b'),('c',Eid 4 'a')]"
      show (divergent b) `shouldBe` "fromList [('a',Eid 2 'a')]"
      show (divergent c) `shouldBe` "fromList [('b',Eid 2 'a')]"

      do {- A world where c leaves.  -}
        let (_eid, urEventFold -> r) = disassociate 'c' c
        -- show (urEventFold r) `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\")),(Eid 5 'c',(Identity (UnJoin 'c'),fromList \"c\"))]}}"
        (allParticipants r, projParticipants r, infimumParticipants r) `shouldBe` (Set.fromList "abc", Set.fromList "ab", Set.fromList "ab")

      {- | 'a' sends update to 'b'.  -}
      let Right r = diffMerge 'b' b (fromJust (events 'b' a))
      let b = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 3 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\")),(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"a\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
      -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\"))]}}"
      show (urOutputs r) `shouldBe` "fromList [(Eid 3 'a',-3),(Eid 3 'b',-2)]"
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('b',Eid 4 'b'),('c',Eid 4 'a')]"
      show (divergent b) `shouldBe` "fromList [('c',Eid 4 'a')]"
      show (divergent c) `shouldBe` "fromList [('b',Eid 2 'a')]"

      {- | 'b' sends update to 'a'.  -}
      let Right r = diffMerge 'a' a (fromJust (events 'a' b))
      let a = urEventFold r
      -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
      -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
      -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 2 'a', participants = fromList \"ab\", stateValue = -3}, psEvents = fromList [(Eid 3 'a',(Identity (EventD Inc),fromList \"ac\")),(Eid 4 'a',(Identity (Join 'c'),fromList \"ac\"))]}}"
      show (urOutputs r) `shouldBe` "fromList [(Eid 3 'a',-3),(Eid 3 'b',-2)]"
      urNeedsPropagation r `shouldBe` True
      show (divergent a) `shouldBe` "fromList [('c',Eid 4 'a')]"
      show (divergent b) `shouldBe` "fromList [('c',Eid 4 'a')]"
      show (divergent c) `shouldBe` "fromList [('b',Eid 2 'a')]"
      show a `shouldBe` show b
      infimumValue a `shouldBe` negate 1

      do {-
           World 1, b erroneously sends an update to c using diffMerge,
           dropping (Eid 3 'a') (because b's Diff infimum is (Eid 4 'a')).
         -}
        {-
          TODO I think we can detect this condition and throw an exception
          instead of just doing the wrong thing.
        -}
        {- | 'b' sends update to 'c'.  -}
        let Right r = diffMerge 'c' c (fromJust (events 'c' b))
        let c = urEventFold r
        -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
        -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
        -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 0}, psEvents = fromList []}}"
        show (urOutputs r) `shouldBe` "fromList [(Eid 3 'a',-3),(Eid 4 'b',-2),(Eid 5 'a',-1)]"
        urNeedsPropagation r `shouldBe` True
        show (divergent a) `shouldBe` "fromList [('c',Eid 4 'a')]"
        show (divergent b) `shouldBe` "fromList [('c',Eid 4 'a')]"
        show (divergent c) `shouldBe` "fromList []"

        {- | 'c' sends update to 'a'.  -}
        let Right r = diffMerge 'a' a (fromJust (events 'a' c))
        let a = urEventFold r
        -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
        -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 0}, psEvents = fromList []}}"
        show (urOutputs r) `shouldBe` "fromList [(Eid 4 'b',-1),(Eid 5 'a',0)]"
        urNeedsPropagation r `shouldBe` True
        show (divergent a) `shouldBe` "fromList []"
        show (divergent b) `shouldBe` "fromList [('c',Eid 4 'a')]"
        show (divergent c) `shouldBe` "fromList []"

        {- | 'a' sends update to 'b'.  -}
        let Right r = diffMerge 'b' b (fromJust (events 'b' a))
        let b = urEventFold r
        -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 0}, psEvents = fromList []}}"
        show (urOutputs r) `shouldBe` "fromList [(Eid 4 'b',-1),(Eid 5 'a',0)]"
        urNeedsPropagation r `shouldBe` True
        show (divergent a) `shouldBe` "fromList []"
        show (divergent b) `shouldBe` "fromList []"
        show (divergent c) `shouldBe` "fromList []"
        -- show a `shouldBe` show b
        -- show a `shouldNotBe` show c

      do {- World 2, b does the right thing and updates c using fullMerge.  -}
        {- | 'b' sends update to 'c'.  -}
        let Right r = fullMerge 'c' c b
        let c = urEventFold r
        -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
        -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
        -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        show (urOutputs r) `shouldBe` "fromList [(Eid 4 'b',-1),(Eid 5 'a',0)]"
        urNeedsPropagation r `shouldBe` True
        show (divergent a) `shouldBe` "fromList [('c',Eid 4 'a')]"
        show (divergent b) `shouldBe` "fromList [('c',Eid 4 'a')]"
        show (divergent c) `shouldBe` "fromList []"

        {- | 'c' sends update to 'a'.  -}
        let Right r = diffMerge 'a' a (fromJust (events 'a' c))
        let a = urEventFold r
        -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 4 'a', participants = fromList \"abc\", stateValue = -1}, psEvents = fromList [(Eid 4 'b',(Identity (EventD Inc),fromList \"ab\")),(Eid 5 'a',(Identity (EventD Inc),fromList \"ab\"))]}}"
        -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        show (urOutputs r) `shouldBe` "fromList [(Eid 4 'b',-1),(Eid 5 'a',0)]"
        urNeedsPropagation r `shouldBe` True
        show (divergent a) `shouldBe` "fromList []"
        show (divergent b) `shouldBe` "fromList [('c',Eid 4 'a')]"
        show (divergent c) `shouldBe` "fromList []"

        {- | 'a' sends update to 'b'.  -}
        let Right r = diffMerge 'b' b (fromJust (events 'b' a))
        let b = urEventFold r
        -- show a `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        -- show b `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        -- show c `shouldBe` "EventFold {unEventFold = EventFoldF {psOrigin = 0, psInfimum = Infimum {eventId = Eid 5 'a', participants = fromList \"abc\", stateValue = 1}, psEvents = fromList []}}"
        show (urOutputs r) `shouldBe` "fromList [(Eid 4 'b',-1),(Eid 5 'a',0)]"
        urNeedsPropagation r `shouldBe` True
        show (divergent a) `shouldBe` "fromList []"
        show (divergent b) `shouldBe` "fromList []"
        show (divergent c) `shouldBe` "fromList []"
        show a `shouldBe` show b
        show a `shouldBe` show c

  describe "divergent" $ do
    it "marks an unacked join as divergent" $ do
      let
        a = new 0 'a' :: EventFold Int Char Ops
        b = urEventFold . snd . participate 'a' 'b' $ a
      divergent b `shouldBe` Map.fromList [('b', bottomEid)]
    it "does not mark an acked join as divergent" $ do
      let
        a = new 0 'a' :: EventFold Int Char Ops
        b = urEventFold . snd . participate 'a' 'b' $ a
        c = urEventFold . snd . participate 'a' 'c' $ b
        d = urEventFold . acknowledge 'c' $ c
      divergent d `shouldBe` Map.fromList [('b', bottomEid)]

  describe "events" $ do
    it "balks on infimum invalidity" $ do
      let
        b =
          let
            a = new 0 'a' :: EventFold Int Char Ops
          in
            urEventFold . snd . participate 'a' 'b' $ a
      events 'b' b `shouldBe` Nothing
      (const () <$> events 'a' b) `shouldBe` Just ()

    it "false-renegade" $ do
      {-
        This tests for the bug where UnJoin may not work because we
        falsely believe that the UnJoin event is a renegade when it gets
        reduced in the same batch as its own Join.
      -}
      let
        a = new 0 'a' :: EventFold Int Char Ops
        b = urEventFold . snd . participate 'a' 'b' $ a
        c = urEventFold . snd . disassociate 'b' $ b
      projParticipants c `shouldBe` Set.fromList "a"

    it "balks on unjoin/join invalidity" $ do
      let
        d =
          let
            a = new 0 'a' :: EventFold Int Char Ops
            b = urEventFold . snd . participate 'a' 'b' $ a
            c = urEventFold . snd . disassociate 'b' $ b
          in
            urEventFold . snd . participate 'a' 'b' $ c
      events 'b' d `shouldBe` Nothing
      (const () <$> events 'a' d) `shouldBe` Just ()
    it "succeeds in the usual case" $ do
      let
        c =
          let
            a = new 0 'a' :: EventFold Int Char Ops
            b = urEventFold . snd . participate 'a' 'b' $ a
          in
            urEventFold . acknowledge 'b' $ b
      (const () <$> events 'a' c) `shouldBe` Just ()
      (const () <$> events 'b' c) `shouldBe` Just ()


