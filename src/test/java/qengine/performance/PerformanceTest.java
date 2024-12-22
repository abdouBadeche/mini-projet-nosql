package qengine.performance;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import fr.boreal.model.query.api.Query;
import qengine.dictionary.Dictionary;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

/**
 * Classe de test pour évaluer les performances de la classe RDFHexaStore.
 * Effectue des tests de chargement de données et d'exécution de requêtes pour mesurer les temps d'exécution et l'utilisation de la mémoire.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PerformanceTest {
    // Constantes pour les chemins de fichiers
    private static final String DATA_FILE_NAME = "100K.nt";
    private static final String QUERY_FILE_NAME = "STAR_ALL_workload.queryset";
    
    private static final int WARM_UP_ITERATIONS = 1000;
    private static final int TEST_ITERATIONS = 5;
    private static final int OUTLIER_THRESHOLD = 2;
    
    // Variables d'instance
    private RDFHexaStore hexaStore;
    private Dictionary dictionary;
    private List<Query> queries;
    private Map<String, List<Long>> operationTimes;
    private long startMemory;
    private long peakMemory;
    private File dataFile;
    private File queryFile;

    /**
     * Méthode exécutée avant chaque test.
     * Initialise une nouvelle instance de Dictionary et RDFHexaStore.
     * Prépare également les variables pour les mesures de performance et initialise les fichiers de test.
     */
    @BeforeEach
    void setUp() {
        dictionary = new Dictionary();
        hexaStore = new RDFHexaStore(dictionary);
        operationTimes = new HashMap<>();
        startMemory = getUsedMemory();
        peakMemory = startMemory;
        queries = new ArrayList<>();
        initializeFiles();
    }

    /**
     * Initialise les fichiers de données et de requêtes.
     * Vérifie que les fichiers existent et les charge dans les variables d'instance.
     */
    private void initializeFiles() {
        try {
            String resourcesPath = Paths.get("src", "test", "resources").toString();
            dataFile = Paths.get(resourcesPath, DATA_FILE_NAME).toFile();
            queryFile = Paths.get(resourcesPath, QUERY_FILE_NAME).toFile();
            
            if (!dataFile.exists() || !queryFile.exists()) {
                throw new IllegalStateException("Fichiers de test non trouvés");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Erreur lors de l'initialisation des fichiers", e);
        }
    }

    /**
     * Teste les performances du chargement de données.
     * Effectue des opérations de chargement pour mesurer les temps d'exécution.
     */
    @Test
    @Order(1)
    void testLoadingPerformance() {
        List<Long> loadTimes = new ArrayList<>();

        // Phase de préchauffage (warm-up)
        for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
            try (RDFAtomParser parser = new RDFAtomParser(dataFile)) {
                parser.next();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Phase de test
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            dictionary = new Dictionary();
            hexaStore = new RDFHexaStore(dictionary);
            
            long startTime = System.nanoTime();
            loadData();
            long endTime = System.nanoTime();
            
            loadTimes.add(TimeUnit.NANOSECONDS.toMillis(endTime - startTime));
            updatePeakMemory();
        }

        operationTimes.put("Data Loading", loadTimes);
        printPerformanceStats("Data Loading Performance", loadTimes);
    }

    /**
     * Teste les performances de l'exécution des requêtes.
     * Charge les données et les requêtes, puis exécute les requêtes pour mesurer les temps d'exécution.
     */
    @Test
    @Order(2)
    void testQueryPerformance() {
        loadData();
        loadQueries();
        
        List<Long> queryTimes = new ArrayList<>();
        
        // Phase de préchauffage (warm-up)
        if (!queries.isEmpty()) {
            Query warmupQuery = queries.get(0);
            for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
                if (warmupQuery instanceof StarQuery) {
                    hexaStore.match((StarQuery) warmupQuery);
                }
            }
        }

        // Phase de test
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            Collections.shuffle(queries);
            
            long startTime = System.nanoTime();
            for (Query query : queries) {
                if (query instanceof StarQuery) {
                    hexaStore.match((StarQuery) query);
                }
            }
            long endTime = System.nanoTime();
            
            queryTimes.add(TimeUnit.NANOSECONDS.toMillis(endTime - startTime));
            updatePeakMemory();
        }

        operationTimes.put("Query Execution", queryTimes);
        printPerformanceStats("Query Performance", queryTimes);
    }

    /**
     * Charge les données RDF à partir du fichier spécifié.
     * Utilise RDFAtomParser pour lire et ajouter les triplets RDF au stockage.
     */
    private void loadData() {
        try (RDFAtomParser parser = new RDFAtomParser(dataFile)) {
            parser.getRDFAtoms().forEach(hexaStore::add);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Charge les requêtes étoilées à partir du fichier spécifié.
     * Utilise StarQuerySparQLParser pour lire les requêtes et les stocker dans la liste des requêtes.
     */
    private void loadQueries() {
        try {
            StarQuerySparQLParser queryParser = new StarQuerySparQLParser(queryFile.getAbsolutePath());
            while (queryParser.hasNext()) {
                queries.add(queryParser.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Affiche les statistiques de performance pour une opération donnée.
     * Calcule et affiche la moyenne, la médiane, l'écart-type et l'utilisation de la mémoire.
     *
     * @param operation Nom de l'opération
     * @param times Liste des temps d'opération
     */
    private void printPerformanceStats(String operation, List<Long> times) {
        if (times.isEmpty()) return;

        double[] stats = calculateRobustStatistics(times);
        System.out.printf("""
            
            %s:
            Average time: %.2f ms
            Median time: %.2f ms
            Standard deviation: %.2f ms
            Memory usage: %.2f MB
            Sample size: %d
            """,
            operation,
            stats[0],
            stats[1],
            stats[2],
            (peakMemory - startMemory) / (1024.0 * 1024.0),
            times.size()
        );
    }

    /**
     * Calcule des statistiques robustes pour une liste de temps.
     * Supprime les valeurs aberrantes et calcule la moyenne, la médiane et l'écart-type.
     *
     * @param times Liste des temps à analyser
     * @return Un tableau contenant la moyenne, la médiane et l'écart-type
     */
    private double[] calculateRobustStatistics(List<Long> times) {
        List<Long> sortedTimes = new ArrayList<>(times);
        Collections.sort(sortedTimes);

        double mean = sortedTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        double stdDev = calculateStdDev(sortedTimes, mean);
        List<Long> filteredTimes = new ArrayList<>();

        for (Long time : sortedTimes) {
            if (Math.abs(time - mean) <= OUTLIER_THRESHOLD * stdDev) {
                filteredTimes.add(time);
            }
        }

        double robustMean = filteredTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        double robustMedian = calculateMedian(filteredTimes);
        double robustStdDev = calculateStdDev(filteredTimes, robustMean);

        return new double[]{robustMean, robustMedian, robustStdDev};
    }

    /**
     * Calcule la médiane d'une liste de valeurs.
     *
     * @param values Liste des valeurs à analyser
     * @return Médiane des valeurs
     */
    private double calculateMedian(List<Long> values) {
        int size = values.size();
        if (size == 0) return 0;
        
        if (size % 2 == 0) {
            return (values.get(size / 2 - 1) + values.get(size / 2)) / 2.0;
        } else {
            return values.get(size / 2);
        }
    }

    /**
     * Calcule l'écart-type d'une liste de valeurs.
     *
     * @param values Liste des valeurs à analyser
     * @param mean Moyenne des valeurs
     * @return Écart-type des valeurs
     */
    private double calculateStdDev(List<Long> values, double mean) {
        return Math.sqrt(values.stream()
            .mapToDouble(v -> Math.pow(v - mean, 2))
            .average()
            .orElse(0.0));
    }

    /**
     * Calcule la mémoire utilisée par le programme.
     * Utilisée pour mesurer l'utilisation de la mémoire lors des tests.
     *
     * @return Mémoire utilisée en bytes
     */
    private long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * Met à jour la mémoire peak utilisée par le programme.
     * Utilisée pour suivre la mémoire maximale utilisée lors des tests.
     */
    private void updatePeakMemory() {
        long currentMemory = getUsedMemory();
        peakMemory = Math.max(peakMemory, currentMemory);
    }
}