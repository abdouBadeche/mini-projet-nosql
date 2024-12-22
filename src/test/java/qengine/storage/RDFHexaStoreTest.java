package qengine.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.logicalElements.impl.ConstantImpl;
import fr.boreal.model.logicalElements.impl.VariableImpl;
import qengine.dictionary.Dictionary;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

/**
 * Classe de test pour la classe RDFHexaStore.
 * Vérifie les fonctionnalités d'ajout, de recherche de motifs, de gestion des requêtes étoilées et de récupération des atomes.
 */
class RDFHexaStoreTest {

    // Instance de RDFHexaStore utilisée pour les tests
    private RDFHexaStore hexaStore;
    // Dictionnaire utilisé pour l'encodage et le décodage des termes
    private Dictionary dictionary;
    
    // Constantes pour les tests de performance
    private static final int WARM_UP_ITERATIONS = 1000;
    private static final int TEST_ITERATIONS = 5000;
    private static final int CONCURRENT_THREADS = 4;
    private static final double OUTLIER_THRESHOLD = 2.0; // Pour écart-type
    
    // Variables pour les mesures de performance
    private List<Long> executionTimes;
    private long startMemory;
    private long peakMemory;

    /**
     * Méthode exécutée avant chaque test.
     * Initialise une nouvelle instance de Dictionary et RDFHexaStore.
     * Prépare également les variables pour les mesures de performance.
     */
    @BeforeEach
    void setUp() {
        dictionary = new Dictionary();
        hexaStore = new RDFHexaStore(dictionary);
        executionTimes = new ArrayList<>();
        startMemory = getUsedMemory();
        peakMemory = startMemory;
    }

    /**
     * Teste les méthodes add et size de la classe RDFHexaStore.
     * Vérifie que les atomes sont correctement ajoutés et que la taille est mise à jour.
     */
    @Test
    void testAddAndSize() {
        // Crée deux atomes RDF
        RDFAtom atom1 = createAtom("Alice", "knows", "Bob");
        RDFAtom atom2 = createAtom("Bob", "knows", "Charlie");

        // Ajoute le premier atome et vérifie que la taille est 1
        assertTrue(hexaStore.add(atom1));
        assertEquals(1, hexaStore.size());

        // Ajoute le deuxième atome et vérifie que la taille est 2
        assertTrue(hexaStore.add(atom2));
        assertEquals(2, hexaStore.size());

        // Teste l'ajout d'un atome dupliqué et vérifie que la taille ne change pas
        assertFalse(hexaStore.add(atom1));
        assertEquals(2, hexaStore.size());
    }

    /**
     * Teste la méthode match de la classe RDFHexaStore pour un motif simple.
     * Vérifie que les substitutions correspondantes sont correctement trouvées.
     */
    @Test
    void testSimpleMatch() {
        // Crée et ajoute un atome RDF
        RDFAtom atom = createAtom("Alice", "knows", "Bob");
        hexaStore.add(atom);

        // Crée un motif RDF avec une variable pour le sujet
        Variable x = new VariableImpl("X");
        RDFAtom pattern = new RDFAtom(
            x,
            new ConstantImpl("knows"),
            new ConstantImpl("Bob")
        );

        // Recherche les substitutions correspondantes au motif
        Iterator<Substitution> results = hexaStore.match(pattern);
        assertTrue(results.hasNext());
        Substitution sub = results.next();
        // Vérifie que la substitution correspond au sujet "Alice"
        assertEquals("Alice", sub.createImageOf(x).toString());
        assertFalse(results.hasNext());
    }

    /**
     * Teste la méthode match de la classe RDFHexaStore pour plusieurs motifs correspondants.
     * Vérifie que toutes les substitutions correspondantes sont trouvées.
     */
    @Test
    void testMultipleMatches() {
        // Ajoute plusieurs atomes RDF
        hexaStore.add(createAtom("Alice", "knows", "Bob"));
        hexaStore.add(createAtom("Bob", "knows", "Charlie"));
        hexaStore.add(createAtom("Charlie", "knows", "David"));

        // Crée un motif RDF avec des variables pour le sujet et l'objet
        Variable x = new VariableImpl("X");
        Variable y = new VariableImpl("Y");
        RDFAtom pattern = new RDFAtom(x, new ConstantImpl("knows"), y);

        // Recherche les substitutions correspondantes au motif
        Iterator<Substitution> results = hexaStore.match(pattern);
        int count = 0;
        while (results.hasNext()) {
            results.next();
            count++;
        }
        // Vérifie que trois substitutions sont trouvées
        assertEquals(3, count);
    }

    /**
     * Teste la méthode match de la classe RDFHexaStore pour une requête étoilée.
     * Vérifie que les substitutions correspondantes sont correctement trouvées pour plusieurs atomes.
     */
    @Test
    void testStarQuery() {
        // Ajoute des données de test
        hexaStore.add(createAtom("Alice", "knows", "Bob"));
        hexaStore.add(createAtom("Alice", "likes", "Charlie"));
        hexaStore.add(createAtom("Alice", "works", "Company"));

        // Crée une variable centrale qui sera partagée par tous les atomes
        Variable x = new VariableImpl("X");
        Variable y1 = new VariableImpl("Y1");
        Variable y2 = new VariableImpl("Y2");

        // Tous les atomes doivent partager la variable x
        List<RDFAtom> atoms = Arrays.asList(
            new RDFAtom(x, new ConstantImpl("knows"), y1),
            new RDFAtom(x, new ConstantImpl("likes"), y2)
        );

        StarQuery query = new StarQuery("TestQuery", atoms, Collections.singleton(x));
        Iterator<Substitution> results = hexaStore.match(query);

        assertTrue(results.hasNext());
        Substitution sub = results.next();
        assertEquals("Alice", sub.createImageOf(x).toString());
        assertFalse(results.hasNext());
    }

    /**
     * Teste la méthode match de la classe RDFHexaStore pour un motif sans correspondance.
     * Vérifie que l'itérateur retourné est vide.
     */
    @Test
    void testEmptyMatch() {
        // Crée et ajoute un atome RDF
        RDFAtom atom = createAtom("Alice", "knows", "Bob");
        hexaStore.add(atom);

        // Crée un motif RDF avec un prédicat différent
        Variable x = new VariableImpl("X");
        RDFAtom pattern = new RDFAtom(
            x,
            new ConstantImpl("likes"), // Prédicat différent
            new ConstantImpl("Bob")
        );

        // Recherche les substitutions correspondantes au motif
        Iterator<Substitution> results = hexaStore.match(pattern);
        assertFalse(results.hasNext());
    }

    /**
     * Teste la méthode getAtoms de la classe RDFHexaStore.
     * Vérifie que tous les atomes RDF stockés sont correctement récupérés.
     */
    @Test
    void testGetAtoms() {
        // Crée deux atomes RDF
        RDFAtom atom1 = createAtom("Alice", "knows", "Bob");
        RDFAtom atom2 = createAtom("Bob", "knows", "Charlie");

        // Ajoute les atomes au stockage
        hexaStore.add(atom1);
        hexaStore.add(atom2);

        // Vérifie que la taille du stockage est 2
        assertEquals(2, hexaStore.getAtoms().size());
        // Vérifie que les atomes ajoutés sont présents dans le stockage
        assertTrue(hexaStore.getAtoms().contains(atom1));
        assertTrue(hexaStore.getAtoms().contains(atom2));
    }

    /**
     * Méthode utilitaire pour créer un atome RDF.
     *
     * @param subject   Le sujet du triplet RDF.
     * @param predicate Le prédicat du triplet RDF.
     * @param object    L'objet du triplet RDF.
     * @return Un nouvel atome RDF avec les termes spécifiés.
     */
    private RDFAtom createAtom(String subject, String predicate, String object) {
        Term s = new ConstantImpl(subject);
        Term p = new ConstantImpl(predicate);
        Term o = new ConstantImpl(object);
        return new RDFAtom(s, p, o);
    }

    /**
     * Teste les opérations de base avec mesures de performance.
     * Effectue des opérations d'ajout et de recherche pour mesurer les temps d'exécution.
     */
    @Test
    @Order(1)
    void testBasicOperationsWithPerformance() {
        // Phase de préchauffage (warm-up)
        for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
            RDFAtom warmupAtom = createAtom("warmup" + i, "knows", "warmup" + (i + 1));
            hexaStore.add(warmupAtom);
        }

        // Phase de test
        List<Long> addTimes = new ArrayList<>();
        List<Long> matchTimes = new ArrayList<>();

        for (int i = 0; i < TEST_ITERATIONS; i++) {
            RDFAtom atom = createAtom("subject" + i, "predicate" + i, "object" + i);
            
            // Mesure du temps d'ajout
            long startAdd = System.nanoTime();
            boolean added = hexaStore.add(atom);
            long endAdd = System.nanoTime();
            addTimes.add(endAdd - startAdd);
            
            assertTrue(added, "L'ajout devrait réussir");
            
            // Mesure du temps de recherche
            long startMatch = System.nanoTime();
            Iterator<Substitution> results = hexaStore.match(atom);
            while (results.hasNext()) results.next();
            long endMatch = System.nanoTime();
            matchTimes.add(endMatch - startMatch);
            
            updatePeakMemory();
        }

        printPerformanceStats("Addition", addTimes);
        printPerformanceStats("Matching", matchTimes);
    }

    /**
     * Teste les opérations concurrentes.
     * Vérifie que la méthode add est thread-safe en exécutant plusieurs threads simultanément.
     *
     * @throws InterruptedException Si l'attente du CountDownLatch est interrompue
     */
    @Test
    @Order(2)
    void testConcurrentOperations() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS);
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        ConcurrentHashMap<String, List<Long>> threadTimes = new ConcurrentHashMap<>();

        for (int t = 0; t < CONCURRENT_THREADS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                List<Long> times = new ArrayList<>();
                try {
                    for (int i = 0; i < TEST_ITERATIONS / CONCURRENT_THREADS; i++) {
                        RDFAtom atom = createAtom(
                            "thread" + threadId + "_s" + i,
                            "thread" + threadId + "_p" + i,
                            "thread" + threadId + "_o" + i
                        );
                        
                        long start = System.nanoTime();
                        hexaStore.add(atom);
                        long end = System.nanoTime();
                        times.add(end - start);
                    }
                } finally {
                    threadTimes.put("Thread-" + threadId, times);
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Test concurrent timeout");
        executor.shutdown();

        // Analyse des résultats par thread
        threadTimes.forEach((thread, times) -> {
            printPerformanceStats(thread, times);
        });
    }

    /**
     * Teste les performances des requêtes étoilées.
     * Prépare des données de test et exécute des requêtes étoilées pour mesurer les temps d'exécution.
     */
    @Test
    @Order(3)
    void testStarQueryPerformance() {
        // Préparation des données
        prepareTestData();

        Variable x = new VariableImpl("X");
        Variable y1 = new VariableImpl("Y1");
        Variable y2 = new VariableImpl("Y2");

        List<Long> queryTimes = new ArrayList<>();

        // Phase de préchauffage (warm-up) pour les requêtes
        for (int i = 0; i < WARM_UP_ITERATIONS; i++) {
            executeWarmupQuery(x, y1, y2);
        }

        // Phase de test
        for (int i = 0; i < TEST_ITERATIONS; i++) {
            List<RDFAtom> atoms = Arrays.asList(
                new RDFAtom(x, new ConstantImpl("knows"), y1),
                new RDFAtom(x, new ConstantImpl("likes"), y2)
            );
            StarQuery query = new StarQuery("TestQuery" + i, atoms, Collections.singleton(x));

            long start = System.nanoTime();
            Iterator<Substitution> results = hexaStore.match(query);
            while (results.hasNext()) results.next();
            long end = System.nanoTime();

            queryTimes.add(end - start);
            updatePeakMemory();
        }

        printPerformanceStats("Star Query", queryTimes);
    }

    // Méthodes utilitaires

    /**
     * Prépare des données de test pour les requêtes étoilées.
     * Ajoute des triplets RDF pour simuler un scénario de test.
     */
    private void prepareTestData() {
        for (int i = 0; i < 1000; i++) {
            hexaStore.add(createAtom("person" + i, "knows", "person" + (i + 1)));
            hexaStore.add(createAtom("person" + i, "likes", "hobby" + i));
        }
    }

    /**
     * Exécute une requête de préchauffage (warm-up) pour les requêtes étoilées.
     * Utilisée pour s'assurer que les structures de données sont prêtes pour les tests.
     *
     * @param x  Variable pour le sujet
     * @param y1 Variable pour le premier objet
     * @param y2 Variable pour le deuxième objet
     */
    private void executeWarmupQuery(Variable x, Variable y1, Variable y2) {
        List<RDFAtom> warmupAtoms = Arrays.asList(
            new RDFAtom(x, new ConstantImpl("knows"), y1),
            new RDFAtom(x, new ConstantImpl("likes"), y2)
        );
        StarQuery warmupQuery = new StarQuery("WarmupQuery", warmupAtoms, Collections.singleton(x));
        Iterator<Substitution> results = hexaStore.match(warmupQuery);
        while (results.hasNext()) results.next();
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

        // Conversion en millisecondes et calcul des statistiques
        double[] stats = calculateRobustStatistics(times);
        
        System.out.println("\nPerformance Statistics for " + operation);
        System.out.println("----------------------------------------");
        System.out.printf("Mean execution time: %.2f ms%n", stats[0] / 1_000_000.0);
        System.out.printf("Median execution time: %.2f ms%n", stats[1] / 1_000_000.0);
        System.out.printf("Standard deviation: %.2f ms%n", stats[2] / 1_000_000.0);
        System.out.printf("Memory usage: %.2f MB%n", (peakMemory - startMemory) / (1024.0 * 1024.0));
        System.out.printf("Number of operations: %d%n", times.size());
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

        // Suppression des valeurs aberrantes
        double mean = sortedTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        double stdDev = calculateStdDev(sortedTimes, mean);
        List<Long> filteredTimes = new ArrayList<>();
        
        for (Long time : sortedTimes) {
            if (Math.abs(time - mean) <= OUTLIER_THRESHOLD * stdDev) {
                filteredTimes.add(time);
            }
        }

        // Calcul des statistiques finales
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