module Hasql.TH
  ( -- * Statements

    -- |
    --  Quasiquoters in this category produce Hasql `Statement`s,
    --  checking the correctness of SQL at compile-time.
    --
    --  To extract the information about parameters and results of the statement,
    --  the quoter requires you to explicitly specify the Postgres types for placeholders and results.
    --
    --  Here's an example of how to use it:
    --
    --  Enable the @QuasiQuotes@ language extension in any module that uses
    --  these quasiquoters:
    --
    --  >{-# LANGUAGE QuasiQuotes #-}
    --
    --  >selectUserDetails :: Statement Int32 (Maybe (Text, Text, Maybe Text))
    --  >selectUserDetails =
    --  >  [maybeStatement|
    --  >    select name :: text, email :: text, phone :: text?
    --  >    from "user"
    --  >    where id = $1 :: int4
    --  >    |]
    --
    --  As you can see, it completely eliminates the need to mess with codecs.
    --  The quasiquoters directly produce `Statement`,
    --  which you can then `Data.Profunctor.dimap` over using its `Data.Profunctor.Profunctor` instance to get to your domain types.
    --
    --  == Type mappings
    --
    --  === Primitives
    --
    --  Following is a list of supported Postgres types and their according types on the Haskell end.
    --
    --  - @bool@ - `Bool`
    --  - @int2@ - `Int16`
    --  - @int4@ - `Int32`
    --  - @int8@ - `Int64`
    --  - @float4@ - `Float`
    --  - @float8@ - `Double`
    --  - @numeric@ - `Data.Scientific.Scientific`
    --  - @char@ - `Char`
    --  - @text@ - `Data.Text.Text`
    --  - @bytea@ - `Data.ByteString.ByteString`
    --  - @date@ - `Data.Time.Day`
    --  - @timestamp@ - `Data.Time.LocalTime`
    --  - @timestamptz@ - `Data.Time.UTCTime`
    --  - @time@ - `Data.Time.TimeOfDay`
    --  - @timetz@ - @(`Data.Time.TimeOfDay`, `Data.Time.TimeZone`)@
    --  - @interval@ - `Data.Time.DiffTime`
    --  - @uuid@ - `Data.UUID.UUID`
    --  - @inet@ - @(`Network.IP.Addr.NetAddr` `Network.IP.Addr.IP`)@
    --  - @json@ - `Data.Aeson.Value`
    --  - @jsonb@ - `Data.Aeson.Value`
    --
    --  === Arrays
    --
    --  Array mappings are also supported.
    --  They are specified according to Postgres syntax: by appending one or more @[]@ to the primitive type,
    --  depending on how many dimensions the array has.
    --  On the Haskell end array is mapped to generic `Data.Vector.Generic.Vector`,
    --  allowing you to choose which particular vector implementation to map to.
    --
    --  === Nulls
    --
    --  As you might have noticed in the example,
    --  we introduce one change to the Postgres syntax in the way
    --  the typesignatures are parsed:
    --  we interpret question-marks in them as specification of nullability.
    --  Here's more examples of that:
    --
    --  >>> :t [singletonStatement| select a :: int4? |]
    --  ...
    --    :: Statement () (Maybe Int32)
    --
    --  You can use it to specify the nullability of array elements:
    --
    --  >>> :t [singletonStatement| select a :: int4?[] |]
    --  ...
    --    :: Data.Vector.Generic.Base.Vector v (Maybe Int32) =>
    --       Statement () (v (Maybe Int32))
    --
    --  And of arrays themselves:
    --
    --  >>> :t [singletonStatement| select a :: int4?[]? |]
    --  ...
    --    :: Data.Vector.Generic.Base.Vector v (Maybe Int32) =>
    --       Statement () (Maybe (v (Maybe Int32)))

    --  === Haskell function and constructor targets
    --
    --  A select-list target may be a Haskell function or data constructor
    --  applied to the columns that follow it, written @$name (col1, col2)@.
    --  The columns are decoded as usual and the result of the application
    --  becomes that position of the row.
    --
    --  >>> data User = User { userId :: Int32, userName :: Text }
    --  >>> :t [singletonStatement| select $User (id :: int4, name :: text) from "user" |]
    --  ... :: Statement () User
    --
    --  A name whose first character is upper case is treated as a data
    --  constructor, any other name as an ordinary function, so
    --  @$mkUser (...)@ calls the function @mkUser@. Applications nest, and a
    --  target list may freely mix Haskell targets with plain SQL ones. Writing
    --  no arguments at all, @$defaultUser ()@, is a bare value rather than an
    --  application and consumes no columns.
    --
    --  === Haskell record targets
    --
    --  A target written @$name {field = col, ...}@ builds or updates a record.
    --  An upper-case name is record construction, a lower-case one is record
    --  update of the value that name refers to.
    --
    --  >>> :t [singletonStatement| select $User {userId = id :: int4, userName = name :: text} from "user" |]
    --  ... :: Statement () User
    --
    --  Field values are themselves targets, so they may be nested Haskell
    --  targets rather than plain columns.
    --
    --  === Haskell fields on parameter placeholders
    --
    --  A placeholder may select a field of the Haskell value it is given,
    --  written @$1.$field@. Every distinct @(placeholder, field)@ pair becomes
    --  one parameter of the emitted statement, and the statement's parameter
    --  type is the tuple of the /original/ placeholders, so several fields of
    --  one value cost one parameter rather than several.
    --
    --  >>> data Merchant = Merchant { merchantId :: Int32, signatureKey :: Text }
    --  >>> :{
    --    :t [singletonStatement|
    --         select id :: int4 from "user"
    --         where merchant = $1.$merchantId :: int4
    --           and key = $1.$signatureKey :: text
    --           and status = $2 :: text
    --         |]
    --  :}
    --  ... :: Statement (Merchant, Text) Int32
    --
    --  This requires @OverloadedRecordDot@ at the use site, since the emitted
    --  code selects the fields with it. Repeating a pair reuses its parameter,
    --  and a bare @$1@ is a different parameter from @$1.$field@.

    -- ** Row-parsing statements
    singletonStatement,
    maybeStatement,
    vectorStatement,
    foldStatement,

    -- ** Row-ignoring statements
    resultlessStatement,
    rowsAffectedStatement,

    -- * SQL Strings

    -- |
    -- Text-producing quasiquoters performing no compile-time checking.
    uncheckedSql,
    uncheckedSqlFile,
  )
where

import qualified Data.Text as Text
import qualified Hasql.TH.Construction.Exp as Exp
import qualified Hasql.TH.Extraction.Exp as ExpExtraction
import Hasql.TH.Prelude hiding (exp)
import Language.Haskell.TH.Quote
import Language.Haskell.TH.Syntax
import qualified PostgresqlSyntax as Ast

-- * Helpers

exp :: (String -> Q Exp) -> QuasiQuoter
exp =
  let unsupported _ = fail "Unsupported"
   in \exp -> QuasiQuoter exp unsupported unsupported unsupported

expParser :: (Text -> Either Text Exp) -> QuasiQuoter
expParser parser =
  exp $ \inputString -> either (fail . Text.unpack) return $ parser $ fromString inputString

expPreparableStmtAstParser :: (Ast.PreparableStmt -> Ast.InputParams -> Either Text Exp) -> QuasiQuoter
expPreparableStmtAstParser parser =
  expParser $ \input -> do
    ast <- Ast.parse settings input
    -- 'lower' numbers the placeholders as they fall in the tree that will
    -- actually be rendered - the one with the Haskell targets erased - but
    -- hands back the tree that still carries them, because the row decoder is
    -- built by walking those targets. Erasure happens in
    -- "Hasql.TH.Extraction.Exp" on the way to the SQL text.
    let (loweredAst, inputParams) = Ast.lower ast
    parser loweredAst inputParams
  where
    settings =
      mconcat
        [ Ast.nullabilityMarkers True,
          Ast.haskellTargets True,
          Ast.haskellParamFields True
        ]

-- * Statement

-- |
-- @
-- :: `Statement` params row
-- @
--
-- Statement producing exactly one result row.
--
-- Will cause the running session to fail with the
-- `Hasql.Session.UnexpectedAmountOfRows` error if it's any other.
--
-- === __Examples__
--
-- >>> :t [singletonStatement|select 1 :: int2|]
-- ... :: Statement () Int16
--
-- >>> :{
--   :t [singletonStatement|
--        insert into "user" (email, name)
--        values ($1 :: text, $2 :: text)
--        returning id :: int4
--        |]
-- :}
-- ...
-- ... :: Statement (Text, Text) Int32
--
-- Incorrect SQL:
--
-- >>> :t [singletonStatement|elect 1|]
-- ...
--   |
-- 1 | elect 1
--   | ^
-- ...
singletonStatement :: QuasiQuoter
singletonStatement = expPreparableStmtAstParser (ExpExtraction.undecodedStatement Exp.singleRowResultDecoder)

-- |
-- @
-- :: `Statement` params (Maybe row)
-- @
--
-- Statement producing one row or none.
--
-- === __Examples__
--
-- >>> :t [maybeStatement|select 1 :: int2|]
-- ... :: Statement () (Maybe Int16)
maybeStatement :: QuasiQuoter
maybeStatement = expPreparableStmtAstParser (ExpExtraction.undecodedStatement Exp.rowMaybeResultDecoder)

-- |
-- @
-- :: `Statement` params (`Vector` row)
-- @
--
-- Statement producing a vector of rows.
--
-- === __Examples__
--
-- >>> :t [vectorStatement|select 1 :: int2|]
-- ... :: Statement () (Vector Int16)
vectorStatement :: QuasiQuoter
vectorStatement = expPreparableStmtAstParser (ExpExtraction.undecodedStatement Exp.rowVectorResultDecoder)

-- |
-- @
-- :: `Fold` row folding -> `Statement` params folding
-- @
--
-- Function from `Fold` over rows to a statement producing the result of folding.
-- Use this when you need to aggregate rows customly.
--
-- === __Examples__
--
-- >>> :t [foldStatement|select 1 :: int2|]
-- ... :: Fold Int16 b -> Statement () b
foldStatement :: QuasiQuoter
foldStatement = expPreparableStmtAstParser ExpExtraction.foldStatement

-- |
-- @
-- :: `Statement` params ()
-- @
--
-- Statement producing no results.
--
-- === __Examples__
--
-- >>> :t [resultlessStatement|insert into "user" (name, email) values ($1 :: text, $2 :: text)|]
-- ...
-- ... :: Statement (Text, Text) ()
resultlessStatement :: QuasiQuoter
resultlessStatement = expPreparableStmtAstParser (ExpExtraction.undecodedStatement (const Exp.noResultResultDecoder))

-- |
-- @
-- :: `Statement` params Int64
-- @
--
-- Statement counting the rows it affects.
--
-- === __Examples__
--
-- >>> :t [rowsAffectedStatement|delete from "user" where password is null|]
-- ...
-- ... :: Statement () Int64
rowsAffectedStatement :: QuasiQuoter
rowsAffectedStatement = expPreparableStmtAstParser (ExpExtraction.undecodedStatement (const Exp.rowsAffectedResultDecoder))

-- * SQL Strings

-- |
-- Quoter of a multiline Unicode SQL string,
-- which gets converted into a format ready to be used for declaration of statements.
uncheckedSql :: QuasiQuoter
uncheckedSql = exp $ return . Exp.text . fromString

-- |
-- Read an SQL-file, containing multiple statements,
-- and produce an expression of type 'Text'.
--
-- Allows to store plain SQL in external files and read it at compile time.
--
-- E.g.,
--
-- >migration1 :: Hasql.Session.Session ()
-- >migration1 = Hasql.Session.script [uncheckedSqlFile|migrations/1.sql|]
uncheckedSqlFile :: QuasiQuoter
uncheckedSqlFile = quoteFile uncheckedSql

-- * Tests

-- $
-- >>> :t [maybeStatement| select (password = $2 :: text) :: bool, id :: int4 from "user" where "email" = $1 :: text |]
-- ...
-- ... Statement (Text, Text) (Maybe (Bool, Int32))
--
-- >>> :t [maybeStatement| select id :: int4 from application where pub_key = $1 :: uuid and sec_key_pt1 = $2 :: int8 and sec_key_pt2 = $3 :: int8 |]
-- ...
-- ... Statement (UUID, Int64, Int64) (Maybe Int32)
--
-- >>> :t [singletonStatement| select 1 :: int4 from a left join b on b.id = a.id |]
-- ...
-- ... Statement () Int32
