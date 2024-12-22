package qengine.parser;

import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.query.api.Query;
import org.junit.jupiter.api.Test;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitaires pour la classe StarQuerySparQLParser.
 */
class StarQuerySparQLParserTest {
    private final String sampleQueryFile = "src/test/resources/sample_query.queryset";

    @Test
    void testParseSingleQuery() throws IOException {
        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(sampleQueryFile)) {
            assertTrue(parser.hasNext(), "Le parser devrait avoir une requête disponible.");

            Query query = parser.next();
            assertInstanceOf(StarQuery.class, query, "La requête analysée devrait être une instance de StarQuery.");

            StarQuery starQuery = (StarQuery) query;

            // Vérifier les propriétés de la première requête
            assertEquals("?v0", starQuery.getCentralVariable().label(), "La variable centrale devrait être ?v0.");
            assertEquals(1, starQuery.getRdfAtoms().size(), "La première requête devrait contenir un triplet RDF.");

            RDFAtom atom = starQuery.getRdfAtoms().iterator().next();
            Term subject = atom.getTerms()[0];
            Term predicate = atom.getTerms()[1];
            Term object = atom.getTerms()[2];

            assertInstanceOf(Variable.class, subject, "Le sujet devrait être une variable.");
            assertEquals("http://schema.org/eligibleRegion", predicate.label(), "Le prédicat est incorrect.");
            assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/Country137", object.label(), "L'objet est incorrect.");
        }
    }

    @Test
    void testParseMultipleQueries() throws IOException {
        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(sampleQueryFile)) {
            int queryCount = 0;

            while (parser.hasNext()) {
                Query query = parser.next();
                assertInstanceOf(StarQuery.class, query, "Chaque requête analysée devrait être une instance de StarQuery.");
                StarQuery starQuery = (StarQuery) query;

                // Vérification de la variable centrale
                assertEquals("?v0", starQuery.getCentralVariable().label(), "Toutes les requêtes devraient partager la variable centrale ?v0.");

                // Vérification des triplets RDF
                Collection<RDFAtom> rdfAtoms = starQuery.getRdfAtoms();
                if (queryCount == 0) {
                    assertEquals(1, rdfAtoms.size(), "La première requête devrait contenir un seul triplet RDF.");
                } else if (queryCount == 1) {
                    assertEquals(2, rdfAtoms.size(), "La deuxième requête devrait contenir deux triplets RDF.");
                } else if (queryCount == 2) {
                    assertEquals(3, rdfAtoms.size(), "La troisième requête devrait contenir trois triplets RDF.");
                }

                queryCount++;
            }

            assertEquals(4, queryCount, "Il devrait y avoir 4 requêtes dans le fichier.");
        }
    }

    @Test
    void testParseInvalidQuery() throws IOException {
        String invalidQueryFile = "src/test/resources/invalid_query.queryset";

        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(invalidQueryFile)) {
            // Tester chaque requête invalide
            assertThrows(RuntimeException.class, parser::next, "Une requête invalide devrait lever une exception.");
        }
    }

    @Test
    void testParseInvalidQuerySyntax() throws IOException {
        String invalidQueryFile = "src/test/resources/invalid_syntax.queryset";

        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(invalidQueryFile)) {
            // Vérifie que l'exception est levée pour la première requête invalide
            RuntimeException exception = assertThrows(RuntimeException.class, parser::next,
                    "Une requête invalide devrait lever une exception.");
            assertTrue(exception.getMessage().contains("Erreur lors de l'analyse de la requête"),
                    "Le message d'erreur devrait indiquer un problème de syntaxe.");
        }
    }

    @Test
    void testHasNextWhenNextQueryIsNull() throws IOException {
        String emptyQueryFile = "src/test/resources/empty_query.queryset";

        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(emptyQueryFile)) {
            assertFalse(parser.hasNext(), "Le parser ne devrait pas avoir de requêtes à traiter.");
        }
    }

    @Test
    void testNextWhenNoQueries() throws IOException {
        String emptyQueryFile = "src/test/resources/empty_query.queryset";

        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(emptyQueryFile)) {
            assertThrows(NoSuchElementException.class, parser::next,
                    "Appeler next() sans requêtes disponibles devrait lever une NoSuchElementException.");
        }
    }

    @Test
    void testParseStarQueryWithNoSharedVariable() throws IOException {
        String noSharedVariableFile = "src/test/resources/no_shared_variable.queryset";

        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(noSharedVariableFile)) {
            assertThrows(RuntimeException.class, parser::next,
                    "Une requête sans variables partagées entre les triplets devrait lever une exception.");
        }
    }

}
