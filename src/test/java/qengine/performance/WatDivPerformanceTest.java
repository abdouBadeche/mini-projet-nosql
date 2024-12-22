package qengine.performance;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.Query;
import qengine.dictionary.Dictionary;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

/**
 * Tests de performance selon les exigences du projet
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class WatDivPerformanceTest {
    // Fichiers de test WatDiv
    private static final String[] DATA_FILES = {"500k.nt" , "2m.nt"};
    private static final String QUERY_FILE = "Q_3_location_gender_type_10000.queryset";
    
    private RDFHexaStore hexaStore;
    private Dictionary dictionary;
    private List<Query> queries;
    private Map<String, PerformanceMetrics> results;

    @BeforeEach
    void setUp() {
        dictionary = new Dictionary();
        hexaStore = new RDFHexaStore(dictionary);
        queries = new ArrayList<>();
        results = new HashMap<>();
    }

    /**
     * Test factoriel 2² avec variation de la taille des données et de la mémoire
     */
    @Test
    @Order(1)
    void testFactorial2x2() {
        int[] memoryConfigs = {1024, 4096}; // MB
        
        for (String dataFile : DATA_FILES) {
            for (int memory : memoryConfigs) {
                System.setProperty("java.Xmx", memory + "m");
                runExperiment(dataFile, memory);
            }
        }
    }

    private void runExperiment(String dataFile, int memory) {
        PerformanceMetrics metrics = new PerformanceMetrics();
        
        // 1. Mesure du temps de chargement
        metrics.startMeasure();
        loadData(dataFile);
        metrics.recordLoadTime();
        
        // 2. Chargement des requêtes
        loadQueries();
        
        // 3. Warm-up (30% des requêtes)
        warmUpSystem();
        
        // 4. Exécution des requêtes
        metrics.startMeasure();
        executeQueries();
        metrics.recordQueryTime();
        
        // Enregistrement des résultats
        results.put(dataFile + "_" + memory + "MB", metrics);
        printResults(dataFile, memory, metrics);
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

    private void loadData(String dataFile) {
        try {
            File file = getResourceFile(dataFile);
            try (RDFAtomParser parser = new RDFAtomParser(file)) {
                parser.getRDFAtoms().forEach(hexaStore::add);
            }
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors du chargement des données: " + dataFile, e);
        }
    }

    private void loadQueries() {
        try {
            File queryFile = getResourceFile(QUERY_FILE);
            StarQuerySparQLParser parser = new StarQuerySparQLParser(queryFile.getAbsolutePath());
            while (parser.hasNext()) {
                queries.add(parser.next());
            }
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors du chargement des requêtes", e);
        }
    }

    private void warmUpSystem() {
        if (queries.isEmpty()) return;
        
        int warmUpSize = queries.size() * 30 / 100;
        List<Query> warmUpQueries = new ArrayList<>(queries.subList(0, warmUpSize));
        Collections.shuffle(warmUpQueries);
        
        for (Query query : warmUpQueries) {
            if (query instanceof StarQuery) {
                hexaStore.match((StarQuery) query);
            }
        }
    }

    private void executeQueries() {
        for (Query query : queries) {
            if (query instanceof StarQuery) {
                Iterator<Substitution> results = hexaStore.match((StarQuery) query);
                while (results.hasNext()) {
                    results.next();
                }
            }
        }
    }

    private void printResults(String dataFile, int memory, PerformanceMetrics metrics) {
        System.out.printf("""
            
            === Résultats de Performance ===
            Configuration:
              - Fichier: %s
              - Mémoire: %d MB
            
            Métriques:
              - Temps de chargement: %d ms
              - Temps d'exécution des requêtes: %d ms
              - Utilisation mémoire: %.2f MB
              - Nombre de requêtes: %d
            ============================
            """,
            dataFile,
            memory,
            metrics.getLoadTime(),
            metrics.getQueryTime(),
            metrics.getMemoryUsage() / (1024.0 * 1024.0),
            queries.size()
        );
    }

    private static class PerformanceMetrics {
        private long startTime;
        private long loadTime;
        private long queryTime;
        private long memoryUsage;

        void startMeasure() {
            startTime = System.nanoTime();
            memoryUsage = getUsedMemory();
        }

        void recordLoadTime() {
            loadTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        }

        void recordQueryTime() {
            queryTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        }

        long getLoadTime() { return loadTime; }
        long getQueryTime() { return queryTime; }
        long getMemoryUsage() { return memoryUsage; }

        private long getUsedMemory() {
            Runtime runtime = Runtime.getRuntime();
            return runtime.totalMemory() - runtime.freeMemory();
        }
    }
}