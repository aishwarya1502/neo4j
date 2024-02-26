/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral

import java.util

class CreateUserAdministrationCommandParserTest extends UserAdministrationCommandParserTestBase {

  test("CREATE USER foo SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER $foo SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        paramFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER foo SET PLAINTEXT PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test(s"CREATE USER foo SET PLAINTEXT PASSWORD $pwParamString") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test(s"CREATE USER $paramString SET PASSWORD $pwParamString") {
    parsesTo[Statements](
      ast.CreateUser(
        paramAst,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER `foo` SET PASSwORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER `!#\"~` SeT PASSWORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literal("!#\"~"),
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SeT PASSWORD 'pasS5Wor%d'") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      pw("pasS5Wor%d"),
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSwORD ''") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      passwordEmpty,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test(s"CREATE uSER foo SET PASSWORD $pwParamString") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREaTE USER foo SET PASSWORD 'password' CHANGE REQUIRED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE required") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' CHAngE NOT REQUIRED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(false), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(false), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString SET  PASSWORD CHANGE NOT REQUIRED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(false), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS SUSPENDed") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), Some(true), None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACtiVE") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), Some(false), None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET PASSWORD 'password' SET PASSWORD CHANGE NOT REQUIRED SET   STATuS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(false), Some(true), None),
      ast.IfExistsThrowError
    )(pos))
  }

  test(s"CREATE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER `` SET PASSwORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literalEmpty,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER `f:oo` SET PASSWORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literalFColonOo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsDoNothing
      )(pos)
    )
  }

  test(s"CREATE uSER foo IF NOT EXISTS SET PASSWORD $pwParamString") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsDoNothing
    )(pos))
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsDoNothing
      )(pos)
    )
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsDoNothing
    )(pos))
  }

  test(s"CREATE USER foo IF NOT EXISTS SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsDoNothing
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsReplace
      )(pos)
    )
  }

  test(s"CREATE OR REPLACE uSER foo SET PASSWORD $pwParamString") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsReplace
    )(pos))
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED") {
    parsesTo[Statements](
      ast.CreateUser(
        literalFoo,
        isEncryptedPassword = false,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsReplace
      )(pos)
    )
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsReplace
    )(pos))
  }

  test(s"CREATE OR REPLACE USER foo SET PASSWORD $pwParamString CHANGE REQUIRED SET STATUS SUSPENDED") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      paramPassword,
      ast.UserOptions(Some(true), Some(true), None),
      ast.IfExistsReplace
    )(pos))
  }

  test("CREATE OR REPLACE USER foo IF NOT EXISTS SET PASSWORD 'password'") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsInvalidSyntax
    )(pos))
  }

  test(
    "CREATE USER foo SET ENCRYPTED PASSWORD '1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab'"
  ) {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = true,
      pw("1,04773b8510aea96ca2085cb81764b0a2,75f4201d047191c17c5e236311b7c4d77e36877503fe60b1ca6d4016160782ab"),
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER $foo SET encrYPTEd PASSWORD 'password'") {
    parsesTo[Statements](
      ast.CreateUser(
        paramFoo,
        isEncryptedPassword = true,
        password,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test(s"CREATE USER $paramString SET ENCRYPTED Password $pwParamString") {
    parsesTo[Statements](
      ast.CreateUser(
        paramAst,
        isEncryptedPassword = true,
        paramPassword,
        ast.UserOptions(Some(true), None, None),
        ast.IfExistsThrowError
      )(pos)
    )
  }

  test("CREATE OR REPLACE USER foo SET encrypted password 'sha256,x1024,0x2460294fe,b3ddb287a'") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = true,
      pw("sha256,x1024,0x2460294fe,b3ddb287a"),
      ast.UserOptions(Some(true), None, None),
      ast.IfExistsReplace
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE $db") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(paramDb))),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE OR REPLACE USER foo SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
      ast.IfExistsReplace
    )(pos))
  }

  test("CREATE USER foo IF NOT EXISTS SET password 'password' SET HOME DATABASE db1") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
      ast.IfExistsDoNothing
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET PASSWORD CHANGE NOT REQUIRED SET HOME DAtabase $db") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(false), None, Some(ast.SetHomeDatabaseAction(paramDb))),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE `#dfkfop!`") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("#dfkfop!")))),
      ast.IfExistsThrowError
    )(pos))
  }

  test("CREATE USER foo SET password 'password' SET HOME DATABASE null") {
    parsesTo[Statements](ast.CreateUser(
      literalFoo,
      isEncryptedPassword = false,
      password,
      ast.UserOptions(Some(true), None, Some(ast.SetHomeDatabaseAction(namespacedName("null")))),
      ast.IfExistsThrowError
    )(pos))
  }

  // clause ordering tests

  Seq(
    ("CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1"),
    ("CHANGE REQUIRED", "SET HOME DATABASE db1", "SET STATUS ACTIVE")
  ).foreach {
    case (first: String, second: String, third: String) =>
      test(s"CREATE USER foo SET password 'password' $first $second $third") {
        parsesTo[Statements](ast.CreateUser(
          literalFoo,
          isEncryptedPassword = false,
          password,
          ast.UserOptions(Some(true), Some(false), Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
          ast.IfExistsThrowError
        )(pos))
      }
  }

  Seq("SET PASSWORD CHANGE REQUIRED", "SET STATUS ACTIVE", "SET HOME DATABASE db1")
    .permutations.foreach {
      clauses =>
        test(s"CREATE USER foo SET password 'password' ${clauses.mkString(" ")}") {
          parsesTo[Statements](ast.CreateUser(
            literalFoo,
            isEncryptedPassword = false,
            password,
            ast.UserOptions(Some(true), Some(false), Some(ast.SetHomeDatabaseAction(namespacedName("db1")))),
            ast.IfExistsThrowError
          )(pos))
        }
    }

  // offset/position tests

  test("CREATE command finds password literal at correct offset") {
    parsing[Statements]("CREATE USER foo SET PASSWORD 'password'").shouldVerify { statement =>
      val passwords = statement.folder.findAllByClass[SensitiveStringLiteral].map(l => (l.value, l.position.offset))
      passwords.foreach { case (pw, offset) =>
        withClue("Expecting password = password, offset = 29") {
          util.Arrays.equals(toUtf8Bytes("password"), pw) shouldBe true
          offset shouldBe 29
        }
      }
    }
  }

  test("CREATE command finds password parameter at correct offset") {
    parsing[Statements](s"CREATE USER foo SET PASSWORD $pwParamString").shouldVerify { statement =>
      val passwords = statement.folder.findAllByClass[SensitiveParameter].map(p => (p.name, p.position.offset))
      passwords should equal(Seq("password" -> 29))
    }
  }

  // fails parsing

  test("CREATE USER foo") {
    failsParsing[Statements]
  }

  test("CREATE USER \"foo\" SET PASSwORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER !#\"~ SeT PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER fo,o SET PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER f:oo SET PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD 123") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET ENCRYPTED PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PLAINTEXT PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET ENCRYPTED PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' ENCRYPTED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSwORD 'passwordString'+" + pwParamString + "expressions.Parameter") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD null CHANGE REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo PASSWORD 'password'") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS ACTIVE CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET HOME DATABASE db1 CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET DEFAULT DATABASE db1") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STAUS ACTIVE") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'STAUS': expected \"HOME\", \"PASSWORD\" or \"STATUS\" (line 1, column 45 (offset: 44))"
    )
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS IMAGINARY") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'password' SET STATUS") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET STATUS SUSPENDED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF EXISTS SET PASSWORD 'bar'") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS SET STATUS ACTIVE") {
    failsParsing[Statements]
  }

  test("CREATE USER foo IF NOT EXISTS SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE NOT REQUIRED") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo SET STATUS SUSPENDED") {
    failsParsing[Statements]
  }

  test("CREATE OR REPLACE USER foo SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE 123456") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD 'bar' SET HOME DATABASE #dfkfop!") {
    failsParsing[Statements]
  }

  test("CREATE USER foo SET PASSWORD $password CHANGE NOT REQUIRED SET PASSWORD CHANGE REQUIRED") {
    val exceptionMessage =
      s"""Duplicate SET PASSWORD CHANGE [NOT] REQUIRED clause (line 1, column 60 (offset: 59))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("CREATE USER foo SET PASSWORD $password SET STATUS ACTIVE SET STATUS SUSPENDED") {
    val exceptionMessage =
      s"""Duplicate SET STATUS {SUSPENDED|ACTIVE} clause (line 1, column 58 (offset: 57))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }

  test("CREATE USER foo SET PASSWORD $password SET HOME DATABASE db SET HOME DATABASE db") {
    val exceptionMessage =
      s"""Duplicate SET HOME DATABASE clause (line 1, column 61 (offset: 60))""".stripMargin
    assertFailsWithMessage[Statements](testName, exceptionMessage)
  }
}