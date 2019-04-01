from .util import as_java_uri, as_scala_bloody_optional


class Neo4jConfig(object):
    def __init__(self, ctx, uri, username, password, encryption):
        self._jneo4jConfig = ctx._jvm.org.opencypher.okapi.neo4j.io.Neo4jConfig(
            as_java_uri(ctx._gateway, uri), username, as_scala_bloody_optional(ctx._gateway, password), encryption
        )

    def driver(self):
        return self._jneo4jConfig.driver()

    def session(self):
        return self._jneo4jConfig.session()