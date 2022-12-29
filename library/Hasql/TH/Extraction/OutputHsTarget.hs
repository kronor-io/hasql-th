-- |
-- AST traversal extracting targetted haskell output types.
-- See 'preparableStmt'
module Hasql.TH.Extraction.OutputHsTarget where

import Hasql.TH.Prelude
import PostgresqlSyntax.Ast
import Language.Haskell.TH
import qualified Data.Text
import Control.Monad.State (StateT, evalStateT, get, put)

-- foldable :: Foldable f => (a -> Either Text [Typename]) -> f a -> Either Text [Typename]
foldable fn = fmap join . traverse fn . toList

-- |
-- return HsTargetEl's together with their field names, lengths for function args, and sql targets
-- @
-- [singletonStatemnt|
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
-- @
-- \(uid1, uname, uid2, uid3) -> (mkUser uid1 uname, defaultUser { userId = uid2 }, uid3)
-- @
--
-- Now consider a more complex example with nesting
--
-- @
-- [singletonStatemnt|
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
-- \(uid1, uname, uid2, uid3) -> (mkUser uid1 uname, defaultUser { userId = doSomething uid2 }, uid3)
-- @
--
preparableStmt = \case
  SelectPreparableStmt a -> selectStmt a
  _ -> pure []

-- * Select

selectStmt = \case
  Left a -> selectNoParens a
  Right a -> selectWithParens a

selectNoParens (SelectNoParens _ a _ _ _) = selectClause a

selectWithParens = \case
  NoParensSelectWithParens a -> selectNoParens a
  WithParensSelectWithParens a -> selectWithParens a

selectClause = either simpleSelect selectWithParens

simpleSelect :: SimpleSelect -> Either Text [Exp]
simpleSelect = \case
  NormalSimpleSelect a _ _ _ _ _ _ -> foldable targeting a
  _ -> pure []

targeting :: Targeting -> Either Text [Exp]
targeting = \case
  NormalTargeting htl -> evalStateT (hsTargetList htl) 1
  _ -> pure []


    -- Just (Ast.HsRecord recName) -> do
    --     let hsFields = fromJust . fst <$> b
    --     c <- traverse columnDecoder (snd <$> b)
    --     let decoderExp = Exp.cozip c
    --     pure $ Exp.fmapExp (Exp.tupConsRec recName (coerce hsFields)) decoderExp
    -- Just (Ast.HsFunc funcName) -> do
    --     let numArgs = length b
    --     c <- traverse columnDecoder (snd <$> b)
    --     let decoderExp = Exp.cozip c
    --     pure $ Exp.fmapExp (Exp.tupFunc funcName numArgs) decoderExp

hsTargetList :: Foldable f => f HsTargetEl -> StateT Int (Either Text) [Exp]
hsTargetList = traverse hsTargetEl . toList

hsTargetEl :: HsTargetEl -> StateT Int (Either Text) Exp
hsTargetEl = \case
  HsRecTargetEl consName hftl ->
    let errExps = foldable hsFieldTargetEl hftl
        reifiedName = mkName (Data.Text.unpack consName)
        toUpdate = if isConsOrVar reifiedName
                 then RecConE reifiedName
                 else RecUpdE (VarE reifiedName)
    in toUpdate <$> errExps
  HsFuncTargetEl funcName htl ->
    let reifiedName = mkName (Data.Text.unpack funcName)
        toApp = if isConsOrVar reifiedName
                then ConE reifiedName
                else VarE reifiedName
    in go toApp <$> hsTargetList htl
  SqlTargetEl _ -> do
    i <- get
    put (i+1)
    pure (VarE (mkName ("fn" ++ show i))) -- targetEl  tl
  where
    isConsOrVar name = isUpper (head (nameBase name))
    go fn [] = fn
    -- No args to apply, so assume what you have is a value expression
    -- or a function with no arguments.
    go fn [x] = AppE fn x
    -- go fn [x, y] = AppE (AppE fn x) y
    -- go fn [x, y, z] = AppE (AppE (AppE fn x) y) z
    go fn xs = AppE (go fn (init xs) ) (last xs)

hsFieldTargetEl :: HsFieldTargetEl -> StateT Int (Either Text) [FieldExp]
hsFieldTargetEl (HsFieldEl fn htl)=
    pure . (mkName (Data.Text.unpack fn),) <$> hsTargetEl htl

targetList :: Foldable f => f TargetEl -> Either Text [Typename]
targetList = foldable targetEl

targetEl = \case
  AliasedExprTargetEl a _ -> aExpr a
  ImplicitlyAliasedExprTargetEl a _ -> aExpr a
  ExprTargetEl a -> aExpr a
  AsteriskTargetEl ->
    Left
      "Target of all fields is not allowed, \
      \because it leaves the output types unspecified. \
      \You have to be specific."

valuesClause = foldable (foldable aExpr)

aExpr = \case
  CExprAExpr a -> cExpr a
  TypecastAExpr _ a -> Right [a]
  a -> Left "Result expression is missing a typecast"

cExpr = \case
  InParensCExpr a Nothing -> aExpr a
  a -> Left "Result expression is missing a typecast"
