{-# OPTIONS_GHC -Wno-unused-binds -Wno-unused-imports -Wno-name-shadowing -Wno-incomplete-patterns -Wno-unused-matches -Wno-missing-methods -Wno-unused-record-wildcards -Wno-redundant-constraints -Wno-deprecations -Wno-missing-signatures #-}

-- |
-- AST traversal extracting targetted haskell output types.
-- See 'preparableStmt'
module Hasql.TH.Extraction.OutputHsTarget where

import Control.Monad.Trans.State.Strict (StateT, evalStateT, get, put)
import qualified Data.Text
import Hasql.TH.Prelude
import Language.Haskell.TH
import PostgresqlSyntax

foldable :: (Foldable f, Monad m) => (a -> m [b]) -> f a -> m [b]
foldable fn = fmap join . traverse fn . toList

-- |
-- Return one expression per top-level target of the select list, in order,
-- with the decoded columns referred to as @fn1@, @fn2@, ... left to right.
--
-- @
-- [singletonStatement|
-- select
--     $mkUser (uid::bigint, uname::text)
--   , $defaultUser {userId = uid::bigint}
--   , uid::bigint
--  from label
--  where hid = $1::bigint
-- |]
-- @
--
-- should finally return a function to fmap over decoder like
--
-- @
-- \(fn1, fn2, fn3, fn4) -> (mkUser fn1 fn2, defaultUser { userId = fn3 }, fn4)
-- @
--
-- Now consider a more complex example with nesting
--
-- @
-- [singletonStatement|
-- select
--     $mkUser (uid::bigint, uname::text)
--   , $defaultUser {userId = $doSomethingToUid (uid::bigint) }
--   , uid::bigint
--  from label
--  where hid = $1::bigint
-- |]
-- @
--
-- should make a function
--
-- @
-- \(fn1, fn2, fn3, fn4) -> (mkUser fn1 fn2, defaultUser { userId = doSomethingToUid fn3 }, fn4)
-- @
preparableStmt = \case
  SelectPreparableStmt a -> selectStmt a
  _ -> pure []

-- * Select

selectStmt = \case
  NoParensSelectStmt a -> selectNoParens a
  WithParensSelectStmt a -> selectWithParens a

selectNoParens (SelectNoParens _ a _ _ _) = selectClause a

selectWithParens = \case
  NoParensSelectWithParens a -> selectNoParens a
  WithParensSelectWithParens a -> selectWithParens a

selectClause = \case
  SimpleSelectSelectClause a -> simpleSelect a
  WithParensSelectClause a -> selectWithParens a

simpleSelect :: SimpleSelect -> Either Text [Exp]
simpleSelect = \case
  NormalSimpleSelect a _ _ _ _ _ _ -> foldable targeting a
  _ -> pure []

targeting :: Targeting -> Either Text [Exp]
targeting = \case
  NormalTargeting (HsTargetList a) -> evalStateT (hsTargetList a) 1
  _ -> pure []

hsTargetList :: (Foldable f) => f HsTargetEl -> StateT Int (Either Text) [Exp]
hsTargetList = traverse hsTargetEl . toList

hsTargetEl :: HsTargetEl -> StateT Int (Either Text) Exp
hsTargetEl = \case
  HsRecTargetEl consName hftl ->
    let errExps = foldable hsFieldTargetEl hftl
        reifiedName = mkName (Data.Text.unpack consName)
        toUpdate =
          if isConsOrVar reifiedName
            then RecConE reifiedName
            else RecUpdE (VarE reifiedName)
     in toUpdate <$> errExps
  HsFuncTargetEl funcName htl ->
    let reifiedName = mkName (Data.Text.unpack funcName)
        toApp =
          if isConsOrVar reifiedName
            then ConE reifiedName
            else VarE reifiedName
     in go toApp <$> hsTargetList htl
  SqlTargetEl _ -> do
    i <- get
    put (i + 1)
    pure (VarE (mkName ("fn" ++ show i)))
  where
    isConsOrVar name = isUpper (head (nameBase name))
    -- No args to apply, so assume what you have is a value expression
    -- or a function with no arguments.
    go fn [] = fn
    go fn [x] = AppE fn x
    go fn xs = AppE (go fn (init xs)) (last xs)

hsFieldTargetEl :: HsFieldTargetEl -> StateT Int (Either Text) [FieldExp]
hsFieldTargetEl (HsFieldEl fn htl) =
  pure . (mkName (Data.Text.unpack fn),) <$> hsTargetEl htl
