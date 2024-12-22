package qengine.performance;

// Imports Java standard
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
// Imports JUnit
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.FOQuery;
// Imports Graal
import fr.lirmm.graphik.graal.api.core.Atom;
import fr.lirmm.graphik.graal.api.core.Predicate;
import fr.lirmm.graphik.graal.api.core.Term;
import fr.lirmm.graphik.graal.store.triplestore.rdf4j.RDF4jStore;
// Imports de notre implémentation
import qengine.dictionary.Dictionary;
import qengine.model.DefaultAtom;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

/**
 * Tests de performance comparant notre implémentation avec Graal/RDF4J
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ComparisonPerformanceTest {
    private static final String[] DATA_FILES = {"500k.nt" , "2m.nt"};
    private static final String QUERY_FILE = "STAR_ALL_workload.queryset";

    private RDFHexaStore hexaStore;
    private Dictionary dictionary;
    private RDF4jStore graalStore;
    private SailRepository repository;
    private List<StarQuery> queries;
    private Map<String, ComparativeMetrics> results;

    @BeforeEach
    void setUp() {
        // Force garbage collection avant chaque test
        System.gc();

        dictionary = new Dictionary();
        hexaStore = new RDFHexaStore(dictionary);

        try {
            repository = new SailRepository(new MemoryStore());
            repository.init();
            graalStore = new RDF4jStore(repository);
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors de l'initialisation de Graal Store", e);
        }

        queries = new ArrayList<>();
        results = new HashMap<>();
    }

    @AfterEach
    void tearDown() {
        try {
            if (graalStore != null) {
                graalStore.close();
            }
            if (repository != null) {
                repository.shutDown();
            }
            // Nettoyage explicite
            hexaStore = null;
            dictionary = null;
            queries.clear();
            results.clear();
            System.gc();
        } catch (Exception e) {
            System.err.println("Erreur lors de la fermeture des ressources: " + e.getMessage());
        }
    }

    @Test
    @Order(1)
    void testLoadData() {
        for (String dataFile : DATA_FILES) {
            ComparativeMetrics metrics = new ComparativeMetrics();

            // Test notre implémentation
            metrics.startOurMeasure();
            loadDataOurImpl(dataFile);
            metrics.recordOurLoadTime();

            // Test Graal
            metrics.startGraalMeasure();
            loadDataGraal(dataFile);
            metrics.recordGraalLoadTime();

            results.put(dataFile + "_load", metrics);
            printLoadResults(dataFile, metrics);

            // Nettoyage après chaque fichier
            tearDown();
            setUp();
        }
    }

    @Test
    @Order(2)
    void testQueryExecution() {
        loadQueries();

        for (String dataFile : DATA_FILES) {
            ComparativeMetrics metrics = new ComparativeMetrics();

            // Chargement des données
            loadDataOurImpl(dataFile);
            loadDataGraal(dataFile);

            // Warm-up
            warmUpOurSystem();
            warmUpGraal();

            // Test notre implémentation
            metrics.startOurMeasure();
            executeQueriesOurImpl();
            metrics.recordOurQueryTime();

            // Test Graal
            metrics.startGraalMeasure();
            executeQueriesGraal();
            metrics.recordGraalQueryTime();

            results.put(dataFile + "_query", metrics);
            printQueryResults(dataFile, metrics);

            // Nettoyage après chaque fichier
            tearDown();
            setUp();
        }
    }

    private File getResourceFile(String fileName) {
        try {
            Path resourcesPath = Paths.get("src", "test", "resources");
            File file = resourcesPath.resolve(fileName).toFile();

            if (!file.exists()) {
                throw new IllegalStateException(
                    String.format("Fichier %s non trouvé dans %s",
                                fileName,
                                resourcesPath.toAbsolutePath())
                );
            }
            return file;
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors de l'accès au fichier: " + fileName, e);
        }
    }

    private void loadDataOurImpl(String dataFile) {
        try {
            File file = getResourceFile(dataFile);
            try (RDFAtomParser parser = new RDFAtomParser(file)) {
                parser.getRDFAtoms().forEach(hexaStore::add);
            }
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors du chargement des données (notre impl): " + dataFile, e);
        }
    }

    private void loadDataGraal(String dataFile) {
        try {
            File file = getResourceFile(dataFile);
            try (RDFAtomParser parser = new RDFAtomParser(file)) {
                // Cast RDFAtom vers Atom avant d'ajouter au graalStore
                parser.getRDFAtoms().forEach(rdfAtom -> {
                    try {
                    	Atom atom = convertRDFAtomToGraalAtom(rdfAtom);
                    	graalStore.add(atom);
                    } catch (Exception e) {
                        throw new RuntimeException("Erreur lors de l'ajout d'un atome: " + rdfAtom, e);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors du chargement des données (Graal): " + e + dataFile, e);
        }
    }


    private Atom convertRDFAtomToGraalAtom(RDFAtom rdfAtom) {

        Predicate predicate = (Predicate) rdfAtom.getPredicate(); // À ajuster selon la méthode d'accès

        Term[] termsArray = (Term[]) rdfAtom.getTerms();  // Si getTerms() retourne un tableau
        List<Term> terms = Arrays.asList(termsArray);  // Conversion du tableau en liste


    	Atom atom = new DefaultAtom(predicate, terms);
        // Exemple de conversion fictive, à adapter en fonction de la structure de RDFAtom et Atom
        return atom;
    }

    private void loadQueries() {
        try {
            File queryFile = getResourceFile(QUERY_FILE);
            StarQuerySparQLParser parser = new StarQuerySparQLParser(queryFile.getAbsolutePath());
            while (parser.hasNext()) {
                queries.add((StarQuery) parser.next());
            }
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors du chargement des requêtes", e);
        }
    }

    private void warmUpOurSystem() {
        if (queries.isEmpty()) return;

        int warmUpSize = Math.max(1, queries.size() * 30 / 100);
        List<StarQuery> warmUpQueries = new ArrayList<>(queries.subList(0, warmUpSize));
        Collections.shuffle(warmUpQueries);

        for (StarQuery query : warmUpQueries) {
            hexaStore.match(query);
        }
    }

    private void warmUpGraal() {
        if (queries.isEmpty()) return;

        int warmUpSize = Math.max(1, queries.size() * 30 / 100);
        List<StarQuery> warmUpQueries = new ArrayList<>(queries.subList(0, warmUpSize));
        Collections.shuffle(warmUpQueries);

        for (StarQuery query : warmUpQueries) {
            try {
                // Pour chaque atome de la requête en étoile
                for (RDFAtom atom : query.getRdfAtoms()) {
                    Iterator<Substitution> results = (Iterator<Substitution>) graalStore.match((Atom) atom);
                    while (results.hasNext()) {
                        results.next();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Erreur pendant le warm-up Graal: " + e.getMessage(), e);
            }
        }
    }

    private FOQuery<FOFormulaConjunction> convertToIntegraalQuery(StarQuery starQuery) {
        // Utiliser directement la méthode asFOQuery() de StarQuery
        return starQuery.asFOQuery();
    }

    private void executeQueriesOurImpl() {
        int totalResults = 0;
        long totalTime = 0;

        for (StarQuery query : queries) {
            long startQueryTime = System.nanoTime();
            Iterator<Substitution> results = hexaStore.match(query);
            while (results.hasNext()) {
                results.next();
                totalResults++;
            }
            totalTime += System.nanoTime() - startQueryTime;
        }

        System.out.printf("Notre implémentation - Résultats totaux: %d, Temps moyen par requête: %.2f ms%n",
            totalResults,
            (totalTime / (double) queries.size()) / 1_000_000.0);
    }

    private void executeQueriesGraal() {
        int totalResults = 0;
        long totalTime = 0;

        for (StarQuery query : queries) {
            try {
                long startQueryTime = System.nanoTime();

                // Traiter chaque atome de la requête individuellement
                for (RDFAtom atom : query.getRdfAtoms()) {
                    Iterator<Substitution> results = (Iterator<Substitution>) graalStore.match((Atom) atom);
                    while (results.hasNext()) {
                        results.next();
                        totalResults++;
                    }
                }

                totalTime += System.nanoTime() - startQueryTime;
            } catch (Exception e) {
                throw new RuntimeException("Erreur pendant l'exécution Graal: " + e.getMessage(), e);
            }
        }

        System.out.printf("Graal - Résultats totaux: %d, Temps moyen par requête: %.2f ms%n",
            totalResults,
            (totalTime / (double) queries.size()) / 1_000_000.0);
    }

    private void printLoadResults(String dataFile, ComparativeMetrics metrics) {
        System.out.printf("""
            
            === Résultats de chargement (%s) ===
            Notre implémentation:
              - Temps: %.2f ms
              - Mémoire: %.2f MB
            Graal:
              - Temps: %.2f ms
              - Mémoire: %.2f MB
            ============================
            """,
            dataFile,
            metrics.ourLoadTime / 1_000_000.0,
            metrics.ourMemoryUsage / (1024.0 * 1024.0),
            metrics.graalLoadTime / 1_000_000.0,
            metrics.graalMemoryUsage / (1024.0 * 1024.0)
        );
    }

    private void printQueryResults(String dataFile, ComparativeMetrics metrics) {
        System.out.printf("""
            
            === Résultats d'exécution des requêtes (%s) ===
            Notre implémentation:
              - Temps: %.2f ms
              - Mémoire: %.2f MB
            Graal:
              - Temps: %.2f ms
              - Mémoire: %.2f MB
            ============================
            """,
            dataFile,
            metrics.ourQueryTime / 1_000_000.0,
            metrics.ourMemoryUsage / (1024.0 * 1024.0),
            metrics.graalQueryTime / 1_000_000.0,
            metrics.graalMemoryUsage / (1024.0 * 1024.0)
        );
    }

    private static class ComparativeMetrics {
        private long startTime;
        private long ourLoadTime;
        private long ourQueryTime;
        private long ourMemoryUsage;
        private long graalLoadTime;
        private long graalQueryTime;
        private long graalMemoryUsage;

        void startOurMeasure() {
            System.gc(); // Force GC avant la mesure
            startTime = System.nanoTime();
            ourMemoryUsage = getUsedMemory();
        }

        void startGraalMeasure() {
            System.gc(); // Force GC avant la mesure
            startTime = System.nanoTime();
            graalMemoryUsage = getUsedMemory();
        }

        void recordOurLoadTime() {
            ourLoadTime = System.nanoTime() - startTime;
            ourMemoryUsage = getUsedMemory() - ourMemoryUsage;
        }

        void recordOurQueryTime() {
            ourQueryTime = System.nanoTime() - startTime;
            ourMemoryUsage = getUsedMemory() - ourMemoryUsage;
        }

        void recordGraalLoadTime() {
            graalLoadTime = System.nanoTime() - startTime;
            graalMemoryUsage = getUsedMemory() - graalMemoryUsage;
        }

        void recordGraalQueryTime() {
            graalQueryTime = System.nanoTime() - startTime;
            graalMemoryUsage = getUsedMemory() - graalMemoryUsage;
        }

        private long getUsedMemory() {
            Runtime runtime = Runtime.getRuntime();
            return runtime.totalMemory() - runtime.freeMemory();
        }
    }
}