package qengine.dictionary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.impl.ConstantImpl;
import qengine.model.RDFAtom;

/**
 * Tests optimisés pour la classe Dictionary.
 * Ce fichier contient des tests unitaires et de performance pour la classe Dictionary.
 */
class DictionaryTest {
    private Dictionary dictionary;
    private static final int WARM_UP_ITERATIONS = 1000;
    private static final int TEST_ITERATIONS = 5000;
    private static final int THREAD_COUNT = 4;

    /**
     * Méthode exécutée avant chaque test.
     * Initialise une nouvelle instance de Dictionary et effectue un préchauffage (warm-up)
     * en encodant un certain nombre de termes pour s'assurer que les structures de données
     * sont prêtes pour les tests.
     */
    @BeforeEach
    void setUp() {
        dictionary = new Dictionary();
        // Warm up
        for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
            dictionary.encode("warmup_" + i);
        }
    }

    /**
     * Test basique d'encodage avec mesures de performance.
     * Encode un grand nombre de termes et mesure le temps moyen d'encodage.
     * Vérifie également que chaque terme a un ID positif et que le même terme
     * est toujours encodé avec le même ID.
     */
    @Test
    void testEncode() {
        List<Long> times = new ArrayList<>();
        
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            String term = "test" + i;
            long start = System.nanoTime();
            int id = dictionary.encode(term);
            long end = System.nanoTime();
            times.add(end - start);
            
            assertTrue(id > 0, "L'ID devrait être positif");
            assertEquals(id, dictionary.encode(term), "Le même terme devrait avoir le même ID");
        }

        // Calcul des statistiques robustes
        times.sort(Long::compareTo);
        times = times.subList(TEST_ITERATIONS / 10, TEST_ITERATIONS * 9 / 10); // Enlève les 10% extrêmes
        double avgTime = times.stream().mapToLong(Long::longValue).average().orElse(0.0);
        
        System.out.printf("Temps moyen d'encodage: %.2f ns%n", avgTime);
    }

    /**
     * Test de concurrence.
     * Vérifie que la méthode encode est thread-safe en exécutant plusieurs threads
     * qui encodent des termes simultanément.
     *
     * @throws InterruptedException Si l'attente du CountDownLatch est interrompue
     */
    @Test
    void testConcurrentEncode() throws InterruptedException {
        int termsPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < termsPerThread; i++) {
                        String term = "thread" + threadId + "_term" + i;
                        int id = dictionary.encode(term);
                        assertEquals(id, dictionary.encode(term));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Test de concurrence timeout");
        executor.shutdown();
    }

    /**
     * Test de performance du cache LRU.
     * Encode des termes fréquents et rares pour mesurer les différences de temps d'accès
     * entre les termes déjà dans le cache et ceux qui ne le sont pas.
     * Répété 5 fois pour obtenir des résultats plus fiables.
     */
    @RepeatedTest(5)
    void testCachePerformance() {
        // Prépare des termes fréquents et rares
        List<String> frequentTerms = new ArrayList<>();
        List<String> rareTerms = new ArrayList<>();
        
        for (int i = 0; i < 100; i++) {
            frequentTerms.add("frequent_" + i);
            rareTerms.add("rare_" + i);
        }

        // Warm up avec termes fréquents
        for (String term : frequentTerms) {
            dictionary.encode(term);
        }

        List<Long> cachedTimes = new ArrayList<>();
        List<Long> uncachedTimes = new ArrayList<>();

        // Test de performance
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            // Test termes fréquents (cached)
            String frequentTerm = frequentTerms.get(i % frequentTerms.size());
            long start = System.nanoTime();
            dictionary.encode(frequentTerm);
            long end = System.nanoTime();
            cachedTimes.add(end - start);

            // Test termes rares (uncached)
            String rareTerm = "rare_" + i;
            start = System.nanoTime();
            dictionary.encode(rareTerm);
            end = System.nanoTime();
            uncachedTimes.add(end - start);
        }

        // Calcul des statistiques
        double avgCachedTime = calculateRobustMean(cachedTimes);
        double avgUncachedTime = calculateRobustMean(uncachedTimes);

        System.out.printf("Temps moyen avec cache: %.2f ns%n", avgCachedTime);
        System.out.printf("Temps moyen sans cache: %.2f ns%n", avgUncachedTime);
        
        assertTrue(avgCachedTime < avgUncachedTime, 
                  "Les accès au cache devraient être plus rapides");
    }

    /**
     * Calcule une moyenne robuste en excluant les valeurs extrêmes.
     * Trie la liste des temps, enlève les 10% les plus petits et les 10% les plus grands,
     * puis calcule la moyenne des temps restants.
     *
     * @param times Liste des temps à analyser
     * @return Moyenne robuste des temps
     */
    private double calculateRobustMean(List<Long> times) {
        times.sort(Long::compareTo);
        int start = times.size() / 10;
        int end = times.size() * 9 / 10;
        return times.subList(start, end).stream()
                   .mapToLong(Long::longValue)
                   .average()
                   .orElse(0.0);
    }

    /**
     * Test de performance pour l'encodage des triplets RDF.
     * Encode et décode des triplets RDF pour mesurer les temps d'encodage et de décodage.
     * Vérifie également que les triplets encodés puis décodés sont identiques aux triplets originaux.
     */
    @Test
    void testRDFTripleEncoding() {
        List<RDFAtom> testTriples = generateTestTriples(1000);
        List<Long> encodingTimes = new ArrayList<>();
        List<Long> decodingTimes = new ArrayList<>();

        // Warm up
        for (int i = 0; i < 100; i++) {
            RDFAtom triple = testTriples.get(i % testTriples.size());
            dictionary.encodeTriple(triple);
        }

        // Test d'encodage
        for (RDFAtom triple : testTriples) {
            long start = System.nanoTime();
            RDFAtom encoded = dictionary.encodeTriple(triple);
            long end = System.nanoTime();
            encodingTimes.add(end - start);

            // Test de décodage
            start = System.nanoTime();
            RDFAtom decoded = dictionary.decodeTriple(encoded);
            end = System.nanoTime();
            decodingTimes.add(end - start);

            // Vérification de l'exactitude
            assertEquals(triple.getTripleSubject().toString(), 
                        decoded.getTripleSubject().toString());
            assertEquals(triple.getTriplePredicate().toString(), 
                        decoded.getTriplePredicate().toString());
            assertEquals(triple.getTripleObject().toString(), 
                        decoded.getTripleObject().toString());
        }

        // Statistiques de performance
        printPerformanceStats("Encodage de triplets", encodingTimes);
        printPerformanceStats("Décodage de triplets", decodingTimes);
    }

    /**
     * Test de stress avec grande quantité de données.
     * Encode un grand nombre de termes pour mesurer le temps total et la mémoire utilisée.
     * Calcule également le nombre de termes encodés par seconde.
     */
    @Test
    void testStressLoad() {
        int numTerms = 100_000;
        long memoryBefore = getUsedMemory();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numTerms; i++) {
            dictionary.encode("stress_test_term_" + i);
        }

        long endTime = System.currentTimeMillis();
        long memoryAfter = getUsedMemory();

        System.out.printf("Test de stress:%n");
        System.out.printf("Temps total: %d ms%n", endTime - startTime);
        System.out.printf("Mémoire utilisée: %d MB%n", 
                         (memoryAfter - memoryBefore) / (1024 * 1024));
        System.out.printf("Termes par seconde: %.2f%n", 
                         numTerms / ((endTime - startTime) / 1000.0));
    }

    /**
     * Test des performances du cache.
     * Simule des accès réalistes en utilisant une distribution de Zipf pour les termes.
     * Mesure le taux de succès des accès au cache, le temps moyen par opération et
     * l'utilisation du cache.
     */
    @Test
    void testCacheEfficiency() {
        int numOperations = 10_000;
        int numUniqueTerms = 1_000;
        
        // Génère une distribution de Zipf pour simuler des accès réalistes
        ZipfDistribution zipf = new ZipfDistribution(numUniqueTerms, 1.0);
        List<String> terms = new ArrayList<>();
        
        for (int i = 0; i < numOperations; i++) {
            int index = zipf.sample() - 1;
            terms.add("term_" + index);
        }

        Dictionary.CacheStats beforeStats = dictionary.getCacheStats();
        
        // Exécute les opérations
        long startTime = System.nanoTime();
        terms.forEach(dictionary::encode);
        long endTime = System.nanoTime();

        Dictionary.CacheStats afterStats = dictionary.getCacheStats();

        // Calcul et affichage des statistiques
        double hitRate = (afterStats.getCurrentSize() - beforeStats.getCurrentSize()) 
                        / (double) numOperations;
        double avgTime = (endTime - startTime) / (double) numOperations;

        System.out.printf("Performance du cache:%n");
        System.out.printf("Taux de succès: %.2f%%%n", hitRate * 100);
        System.out.printf("Temps moyen par opération: %.2f ns%n", avgTime);
        System.out.printf("Utilisation du cache: %.2f%%%n", 
                         afterStats.getUtilization() * 100);
    }

    /**
     * Méthodes utilitaires
     */

    /**
     * Génère une liste de triplets RDF pour les tests.
     * Chaque triplet contient un sujet, un prédicat et un objet uniques.
     *
     * @param count Nombre de triplets à générer
     * @return Liste de triplets RDF
     */
    private List<RDFAtom> generateTestTriples(int count) {
        List<RDFAtom> triples = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Term subject = new ConstantImpl("subject" + i);
            Term predicate = new ConstantImpl("predicate" + i);
            Term object = new ConstantImpl("object" + i);
            triples.add(new RDFAtom(subject, predicate, object));
        }
        return triples;
    }

    /**
     * Affiche les statistiques de performance pour une opération donnée.
     * Calcule et affiche la moyenne, l'écart-type et la médiane des temps.
     *
     * @param operation Nom de l'opération
     * @param times Liste des temps d'opération
     */
    private void printPerformanceStats(String operation, List<Long> times) {
        double mean = calculateRobustMean(times);
        double stdDev = calculateStandardDeviation(times);
        long median = calculateMedian(times);
        
        System.out.printf("%s:%n", operation);
        System.out.printf("Moyenne: %.2f ns%n", mean);
        System.out.printf("Écart-type: %.2f ns%n", stdDev);
        System.out.printf("Médiane: %d ns%n", median);
    }

    /**
     * Calcule la médiane d'une liste de temps.
     * Trie la liste et retourne la valeur médiane.
     *
     * @param times Liste des temps à analyser
     * @return Médiane des temps
     */
    private long calculateMedian(List<Long> times) {
        List<Long> sorted = new ArrayList<>(times);
        Collections.sort(sorted);
        return sorted.get(sorted.size() / 2);
    }

    /**
     * Calcule l'écart-type d'une liste de temps.
     * Utilise la formule de l'écart-type pour calculer la dispersion des temps.
     *
     * @param times Liste des temps à analyser
     * @return Écart-type des temps
     */
    private double calculateStandardDeviation(List<Long> times) {
        double mean = times.stream().mapToLong(Long::longValue).average().orElse(0.0);
        double variance = times.stream()
            .mapToDouble(time -> Math.pow(time - mean, 2))
            .average()
            .orElse(0.0);
        return Math.sqrt(variance);
    }

    /**
     * Calcule la mémoire utilisée par le programme.
     * Force le garbage collection pour obtenir une mesure plus précise de la mémoire utilisée.
     *
     * @return Mémoire utilisée en bytes
     */
    private long getUsedMemory() {
        System.gc(); // Force garbage collection pour une mesure plus précise
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * Distribution de Zipf simplifiée pour les tests.
     * Utilisée pour simuler des accès réalistes aux termes, où certains termes sont plus fréquemment
     * accédés que d'autres.
     */
    private static class ZipfDistribution {
        private final int size;
        private final double skew;
        private final Random random;
        private final double bottom;

        /**
         * Constructeur de la distribution de Zipf.
         *
         * @param size Nombre de termes uniques
         * @param skew Paramètre de skew pour la distribution de Zipf
         */
        public ZipfDistribution(int size, double skew) {
            this.size = size;
            this.skew = skew;
            this.random = new Random();
            this.bottom = calculateBottom();
        }

        /**
         * Calcule la valeur de bottom pour la distribution de Zipf.
         * Cette valeur est utilisée pour normaliser la distribution.
         *
         * @return Valeur de bottom
         */
        private double calculateBottom() {
            double bottom = 0.0;
            for (int i = 1; i <= size; i++) {
                bottom += 1.0 / Math.pow(i, skew);
            }
            return bottom;
        }

        /**
         * Échantillonne un terme selon la distribution de Zipf.
         * Retourne un index de terme en fonction de la distribution.
         *
         * @return Index de terme échantillonné
         */
        public int sample() {
            double value = random.nextDouble();
            double sum = 0.0;
            for (int i = 1; i <= size; i++) {
                sum += 1.0 / (Math.pow(i, skew) * bottom);
                if (sum >= value) {
                    return i;
                }
            }
            return size;
        }
    }
}