/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.RemoteBatchPropertiesImplementation
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RemoteBatchPropertiesUsingPlannerPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport with AstConstructionTestSupport {

  private val spdPlanner = plannerBuilder()
    .setDatabaseMode(DatabaseMode.SHARDED)
    .withSetting(
      GraphDatabaseInternalSettings.cypher_remote_batch_properties_implementation,
      RemoteBatchPropertiesImplementation.PLANNER
    )

  // Graph counts based on a subset of LDBC SF 1
  final private val planner = spdPlanner
    .setAllNodesCardinality(3181725)
    .setLabelCardinality("Person", 9892)
    .setLabelCardinality("Message", 3055774)
    .setLabelCardinality("Post", 1003605)
    .setLabelCardinality("Comment", 2052169)
    .setRelationshipCardinality("()-[:KNOWS]->()", 180623)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->()", 180623)
    .setRelationshipCardinality("()-[:KNOWS]->(:Person)", 180623)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->(:Person)", 180623)
    .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("(:Post)-[:POST_HAS_CREATOR]->()", 1003605)
    .setRelationshipCardinality("()-[:POST_HAS_CREATOR]->(:Person)", 1003605)
    .setRelationshipCardinality("(:Message)-[:POST_HAS_CREATOR]->(:Person)", 1003605)
    .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("(:Message)-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("(:Comment)-[:COMMENT_HAS_CREATOR]->()", 2052169)
    .setRelationshipCardinality("()-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
    .setRelationshipCardinality("(:Message)-[:COMMENT_HAS_CREATOR]->(:Person)", 2052169)
    .addNodeIndex("Person", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 9892.0, isUnique = true)
    .addNodeIndex("Person", List("firstName"), existsSelectivity = 1.0, uniqueSelectivity = 1 / 1323.0)
    .addNodeIndex("Message", List("creationDate"), existsSelectivity = 1.0, uniqueSelectivity = 3033542.0 / 3055774.0)
    .build()

  test("should batch node properties") {
    val query =
      """MATCH (person:Person)
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .nodeByLabelScan("person", "Person")
      .build()
  }

  // TODO: this will be fixed in the next commit.
  ignore("should handle renamed variables from within an apply") {
    val query =
      """MATCH (person:Person {id:$Person}) WHERE person.firstName IS NOT NULL
        |CALL {
        |  WITH person
        |  RETURN person AS x
        |}
        |RETURN x.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName")
      .projection(Map(
        "personLastName" -> cachedNodeProp("person", "lastName", "x")
      ))
      .projection("person AS x")
      .filter("cacheN[person.firstName] IS NOT NULL")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test("should not batch node properties for write queries") {
    val query =
      """MATCH (person:Person)
        |CREATE ()
        |RETURN person.firstName AS personFirstName,
        |       person.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "personLastName")
      .projection("cacheN[person.firstName] AS personFirstName", "cacheN[person.lastName] AS personLastName")
      .cacheProperties("cacheNFromStore[person.firstName]", "cacheNFromStore[person.lastName]")
      .create(createNode("anon_0"))
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("should batch relationship properties") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |RETURN knows.creationDate AS knowsSince""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("knowsSince")
      .projection("cacheR[knows.creationDate] AS knowsSince")
      .remoteBatchProperties("cacheRFromStore[knows.creationDate]")
      .filter("person:Person")
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  test("should batch on variables from an unwind") {
    val query = """
      MATCH (n)
      UNWIND [null, n] AS x
      MATCH (x)
      RETURN x.name AS name
      """

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("name")
      .projection("cacheN[x.name] AS name")
      .remoteBatchProperties("cacheNFromStore[x.name]")
      .filterExpression(assertIsNode("x"))
      .unwind("[NULL, n] AS x")
      .allNodeScan("n")
      .build()
  }

  test("should batch on standalone pattern arguments") {
    val planner = spdPlanner
      .setAllNodesCardinality(30)
      .setLabelCardinality("L0", 10)
      .setRelationshipCardinality("()-[]->()", 10000)
      .setRelationshipCardinality("(:L0)-[]->()", 20)
      .addNodeIndex("L0", List("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 10.0)
      .build()

    val query = """
                  |OPTIONAL MATCH (n0)-[r1]->(n1) WHERE n0.prop = 42
                  |MATCH (a:L0 {prop:42})-[r2]->(n1), (n0)
                  |RETURN a.prop AS expandIntoProp, n0.prop AS standaloneProp
                  |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("expandIntoProp", "standaloneProp")
      .projection("cacheN[a.prop] AS expandIntoProp", "cacheN[n0.prop] AS standaloneProp")
      .remoteBatchProperties("cacheNFromStore[a.prop]")
      .expandInto("(a)-[r2]->(n1)")
      .filterExpression(assertIsNode("n0"))
      .apply()
      .|.nodeIndexOperator(
        "a:L0(prop = 42)",
        argumentIds = Set("n0", "n1", "r1"),
        getValue = Map("prop" -> DoNotGetValue)
      ) // TODO: a.prop should get value here but it doesn't know that this is being used in a projection.
      .optional()
      .expandAll("(n0)-[r1]->(n1)")
      .filter("cacheN[n0.prop] = 42")
      .remoteBatchProperties("cacheNFromStore[n0.prop]")
      .allNodeScan("n0")
      .build()
  }

  test("should also batch properties used in filters, even if just once") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |  WHERE person.lastName = friend.lastName AND knows.creationDate < $max_creation_date
        |RETURN person.firstName AS personFirstName,
        |       friend.firstName AS friendFirstName,
        |       knows.creationDate AS knowsSince""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName", "friendFirstName", "knowsSince")
      .projection(
        "cacheN[person.firstName] AS personFirstName",
        "cacheN[friend.firstName] AS friendFirstName",
        "cacheR[knows.creationDate] AS knowsSince"
      )
      .filter(
        "cacheN[person.lastName] = cacheN[friend.lastName]",
        "cacheR[knows.creationDate] < $max_creation_date",
        "person:Person"
      )
      .remoteBatchProperties(
        "cacheNFromStore[person.firstName]",
        "cacheNFromStore[friend.firstName]",
        "cacheRFromStore[knows.creationDate]",
        "cacheNFromStore[person.lastName]",
        "cacheNFromStore[friend.lastName]"
      )
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  test("Should not get value from index if the property isn't reused") {
    val query =
      """MATCH (person:Person {id:$Person})
        |RETURN person.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName")
      .projection("cacheN[person.firstName] AS personFirstName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> DoNotGetValue),
        unique = true
      )
      .build()
  }

  test("Should get value from index if the property used in another predicate") {
    val query =
      """MATCH (person:Person {id:$Person}) WHERE person.id <> 42
        |RETURN person.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personFirstName")
      .projection("cacheN[person.firstName] AS personFirstName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .filter("NOT cacheN[person.id] = 42")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should retrieve properties from indexes where applicable") {
    val query =
      """MATCH (person:Person {id:$Person})
        |RETURN person.id AS personId,
        |       person.firstName AS personFirstName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personId", "personFirstName")
      .projection("cacheN[person.id] AS personId", "cacheN[person.firstName] AS personFirstName")
      .remoteBatchProperties("cacheNFromStore[person.firstName]")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should cache properties with optional match") {
    val query =
      """MATCH (person:Person {id:$Person})-[knows:KNOWS*1..2]-(friend)
        |  WHERE person.firstName = friend.firstName
        |OPTIONAL MATCH (friend)<-[has_creator:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
        |  WHERE message.creationDate IS NOT NULL
        |RETURN message.id AS messageId,
        |       message.creationDate AS messageCreationDate,
        |       friend.firstName AS friendFirstName,
        |       friend.lastName AS friendLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner
        .planBuilder()
        .produceResults("messageId", "messageCreationDate", "friendFirstName", "friendLastName")
        .projection(
          "cacheN[message.id] AS messageId",
          "cacheN[message.creationDate] AS messageCreationDate",
          "cacheN[friend.firstName] AS friendFirstName",
          "cacheN[friend.lastName] AS friendLastName"
        )
        .apply()
        .|.optional("person", "knows", "friend")
        .|.filter("cacheN[message.creationDate] IS NOT NULL")
        .|.remoteBatchProperties("cacheNFromStore[message.creationDate]", "cacheNFromStore[message.id]")
        .|.expandAll("(friend)<-[has_creator:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)")
        .|.argument("friend", "person", "knows")
        .filter("cacheN[person.firstName] = cacheN[friend.firstName]")
        .remoteBatchProperties(
          "cacheNFromStore[person.firstName]",
          "cacheNFromStore[friend.firstName]",
          "cacheNFromStore[friend.lastName]"
        )
        .expand("(person)-[knows:KNOWS*1..2]-(friend)")
        .nodeIndexOperator(
          "person:Person(id = ???)",
          paramExpr = Some(parameter("Person", CTAny)),
          getValue =
            Map("id" -> DoNotGetValue), // context.plannerState.accessedProperties contains `person.id`, we need to list projected values in a different way
          unique = true
        )
        .build()
  }

  test("should cache properties with unions from the or-leaf planner") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS*1..2]-(friend)
        |  WHERE person.id = $Person  OR person.firstName = 'Dave'
        |RETURN
        |       person.firstName AS firstName,
        |       person.lastName AS lastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner
        .planBuilder()
        .produceResults("firstName", "lastName")
        .projection("cacheN[person.firstName] AS firstName", "cacheN[person.lastName] AS lastName")
        .remoteBatchProperties(
          "cacheNFromStore[person.firstName]",
          "cacheNFromStore[person.lastName]"
        ) // note how we retrieve firstName once again
        .expand("(person)-[knows:KNOWS*1..2]-(friend)")
        .distinct("person AS person")
        .union()
        .|.nodeIndexOperator(
          "person:Person(id = ???)",
          paramExpr = Some(parameter("Person", CTAny)),
          getValue = Map("id" -> DoNotGetValue),
          unique = true
        ) // here we get the person id, even though we don't use it later on
        .nodeIndexOperator(
          "person:Person(firstName = 'Dave')",
          getValue = Map("firstName" -> GetValue)
        ) // TODO: we shouldn't get the value only on one side of the union, we need some additional logic
        .build()
  }

  test("should batch properties only on non-conflicting variables on either side of the union") {
    val query =
      """
        |CALL {
        | MATCH (p: Person)
        | WHERE p.firstName = "Smith" AND p.creationDate > $max_creation_date
        | RETURN p
        |UNION ALL
        | MATCH (p: Message)-[:POST_HAS_CREATOR]->(:Person{firstName:"Smith"})
        | WHERE p.creationDate < $max_creation_date
        | RETURN p
        |}
        |RETURN p.creationDate
        |""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("`p.creationDate`")
      .projection("cacheN[p.creationDate] AS `p.creationDate`")
      .remoteBatchProperties("cacheNFromStore[p.creationDate]")
      .union()
      .|.projection("p AS p")
      .|.filter("cacheN[p.creationDate] < $max_creation_date", "p:Message")
      .|.remoteBatchProperties("cacheNFromStore[p.creationDate]")
      .|.expandAll("(anon_1)<-[anon_0:POST_HAS_CREATOR]-(p)")
      .|.nodeIndexOperator("anon_1:Person(firstName = 'Smith')", getValue = Map("firstName" -> DoNotGetValue))
      .projection("p AS p")
      .filter("cacheN[p.creationDate] > $max_creation_date")
      .remoteBatchProperties("cacheNFromStore[p.creationDate]")
      .nodeIndexOperator("p:Person(firstName = 'Smith')", getValue = Map("firstName" -> DoNotGetValue))
      .build()
  }

  test("should batch properties of renamed entities") {
    val query =
      """MATCH (person:Person)
        |  WHERE person.creationDate < $max_creation_date AND person.lastName IS NOT NULL
        |WITH person AS earlyAdopter, person.creationDate AS earlyAdopterSince ORDER BY earlyAdopterSince LIMIT 10
        |MATCH (earlyAdopter)-[knows:KNOWS]->(friend:Person)
        |RETURN earlyAdopter.lastName AS personLastName,
        |       friend.lastName AS friendLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personLastName", "friendLastName")
      .projection(Map(
        "personLastName" -> cachedNodeProp(
          // notice how `originalEntity` and `entityVariable` differ here
          variable = "person",
          propKey = "lastName",
          currentVarName = "earlyAdopter"
        ),
        "friendLastName" -> cachedNodeProp(variable = "friend", propKey = "lastName", currentVarName = "friend")
      ))
      .remoteBatchProperties("cacheNFromStore[friend.lastName]")
      .filter("friend:Person")
      .expandAll("(earlyAdopter)-[knows:KNOWS]->(friend)")
      .projection("person AS earlyAdopter")
      .top(10, "earlyAdopterSince ASC")
      .projection("cacheN[person.creationDate] AS earlyAdopterSince")
      .filter("cacheN[person.creationDate] < $max_creation_date", "cacheN[person.lastName] IS NOT NULL")
      .remoteBatchProperties("cacheNFromStore[person.lastName]", "cacheNFromStore[person.creationDate]")
      .nodeByLabelScan("person", "Person")
      .build()
  }

  test("probably should but currently does not batch properties when returning entire entities") {
    val query =
      """MATCH (person:Person)-[knows:KNOWS]->(friend:Person)
        |RETURN person, friend""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("person", "friend")
      .filter("person:Person")
      .expandAll("(friend)<-[knows:KNOWS]-(person)")
      .nodeByLabelScan("friend", "Person")
      .build()
  }

  test("should batch accessed properties before a selection even if there are no predicates on it") {
    val query =
      """MATCH (person:Person {id:$Person})-[:KNOWS*1..2]-(friend)
        |WHERE NOT person.id = friend.id
        |RETURN friend.id AS personId,
        |       friend.firstName AS personFirstName,
        |       friend.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults("personId", "personFirstName", "personLastName")
      .projection(
        "cacheN[friend.id] AS personId",
        "cacheN[friend.firstName] AS personFirstName",
        "cacheN[friend.lastName] AS personLastName"
      )
      .filter("NOT cacheN[person.id] = cacheN[friend.id]")
      .remoteBatchProperties(
        "cacheNFromStore[friend.id]",
        "cacheNFromStore[friend.firstName]",
        "cacheNFromStore[friend.lastName]"
      )
      .expand("(person)-[anon_0:KNOWS*1..2]-(friend)")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(ExplicitParameter("Person", CTAny)(InputPosition.NONE)),
        getValue = Map("id" -> GetValue),
        unique = true
      )
      .build()
  }

  test("should batch properties in complex enough queries (Query 9 in LDBC SF 1)") {
    val query =
      """MATCH (person:Person {id:$Person})-[:KNOWS*1..2]-(friend)
        |WHERE NOT person=friend
        |WITH DISTINCT friend
        |MATCH (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
        |WHERE message.creationDate < $Date0
        |WITH friend, message
        |ORDER BY message.creationDate DESC, message.id ASC
        |LIMIT 20
        |RETURN message.id AS messageId,
        |       coalesce(message.content,message.imageFile) AS messageContent,
        |       message.creationDate AS messageCreationDate,
        |       friend.id AS personId,
        |       friend.firstName AS personFirstName,
        |       friend.lastName AS personLastName""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual planner
      .planBuilder()
      .produceResults(
        "messageId",
        "messageContent",
        "messageCreationDate",
        "personId",
        "personFirstName",
        "personLastName"
      )
      .projection(
        "cacheN[message.id] AS messageId",
        "cacheN[friend.lastName] AS personLastName",
        "cacheN[friend.id] AS personId",
        "cacheN[message.creationDate] AS messageCreationDate",
        "coalesce(cacheN[message.content], cacheN[message.imageFile]) AS messageContent",
        "cacheN[friend.firstName] AS personFirstName"
      )
      .remoteBatchProperties(
        "cacheNFromStore[message.imageFile]",
        "cacheNFromStore[friend.lastName]",
        "cacheNFromStore[friend.id]",
        "cacheNFromStore[friend.firstName]",
        "cacheNFromStore[message.content]"
      )
      .top(20, "`message.creationDate` DESC", "`message.id` ASC")
      .projection("cacheN[message.creationDate] AS `message.creationDate`", "cacheN[message.id] AS `message.id`")
      .remoteBatchProperties("cacheNFromStore[message.id]")
      .filter("cacheN[message.creationDate] < $Date0")
      .remoteBatchProperties("cacheNFromStore[message.creationDate]")
      .expandAll("(friend)<-[anon_0:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)")
      .projection("friend AS friend")
      .filter("NOT person = friend")
      .bfsPruningVarExpand("(person)-[:KNOWS*1..2]-(friend)")
      .nodeIndexOperator(
        "person:Person(id = ???)",
        paramExpr = Some(parameter("Person", CTAny)),
        unique = true
      )
      .build()
  }
}