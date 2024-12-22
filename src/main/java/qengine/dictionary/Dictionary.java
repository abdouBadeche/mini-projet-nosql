package qengine.dictionary;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.impl.ConstantImpl;
import qengine.model.RDFAtom;

/**
 * Version optimisée du Dictionary avec cache LRU (Least Recently Used) et structures concurrentes.
 * Ce dictionnaire permet de convertir des chaînes de caractères en identifiants uniques et vice versa.
 * Il utilise un cache LRU pour stocker les termes fréquemment utilisés et des structures concurrentes pour
 * assurer la thread-safety et les meilleures performances.
 */
public class Dictionary {
    // Utilisation de ConcurrentHashMap pour thread-safety et meilleures performances
    private final ConcurrentHashMap<String, Integer> stringToId;
    private final ConcurrentHashMap<Integer, String> idToString;
    private final AtomicInteger nextId;
    
    // Cache LRU pour les termes fréquemment utilisés
    private final LRUCache<String, Integer> cache;
    
    // Taille par défaut du cache LRU
    private static final int DEFAULT_CACHE_SIZE = 10000;

    /**
     * Classe interne pour implémenter un cache LRU.
     * Cette classe étend LinkedHashMap avec des paramètres spécifiques pour gérer le cache LRU.
     * Elle surcharge la méthode removeEldestEntry pour supprimer les entrées les plus anciennes
     * lorsque la taille du cache dépasse la taille maximale spécifiée.
     */
    private static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private final int maxSize;

        public LRUCache(int maxSize) {
            // Appel au constructeur de LinkedHashMap avec des paramètres pour le cache LRU
            super(maxSize, 0.75f, true);
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            // Supprimer l'entrée la plus ancienne si la taille du cache dépasse maxSize
            return size() > maxSize;
        }
    }

    /**
     * Constructeur par défaut avec taille de cache par défaut.
     * Initialise les structures de données et le cache avec la taille par défaut.
     */
    public Dictionary() {
        this(DEFAULT_CACHE_SIZE);
    }

    /**
     * Constructeur avec taille de cache spécifiée.
     * Permet de créer un dictionnaire avec une taille de cache personnalisée.
     *
     * @param cacheSize Taille du cache LRU à utiliser
     */
    public Dictionary(int cacheSize) {
        this.stringToId = new ConcurrentHashMap<>();
        this.idToString = new ConcurrentHashMap<>();
        this.nextId = new AtomicInteger(1);
        this.cache = new LRUCache<>(cacheSize);
    }

    /**
     * Encode une chaîne en un identifiant unique avec gestion du cache.
     * Si la chaîne est déjà dans le cache, retourne l'identifiant correspondant.
     * Sinon, crée un nouvel identifiant, stocke la correspondance dans les structures de données
     * et met à jour le cache.
     *
     * @param resource Chaîne à encoder
     * @return Identifiant unique correspondant à la chaîne
     * @throws IllegalArgumentException Si la chaîne est null
     */
    public int encode(String resource) {
        if (resource == null) {
            throw new IllegalArgumentException("Le terme ne peut pas être null");
        }

        // Vérifier d'abord dans le cache
        Integer cachedId = cache.get(resource);
        if (cachedId != null) {
            return cachedId;
        }

        // Si pas dans le cache, utiliser computeIfAbsent pour thread-safety
        int id = stringToId.computeIfAbsent(resource, k -> {
            int newId = nextId.getAndIncrement();
            idToString.put(newId, k);
            return newId;
        });

        // Mettre à jour le cache
        synchronized (cache) {
            cache.put(resource, id);
        }

        return id;
    }

    /**
     * Décode un identifiant unique en une chaîne de caractères.
     * Utilise la structure idToString pour retrouver la chaîne correspondante.
     *
     * @param id Identifiant unique à décoder
     * @return Chaîne de caractères correspondant à l'identifiant
     */
    public String decode(int id) {
        return idToString.get(id);
    }

    /**
     * Encode un triplet RDF de manière optimisée.
     * Convertit chaque élément du triplet (sujet, prédicat, objet) en un identifiant unique
     * en utilisant la méthode encode. Crée ensuite un nouvel RDFAtom avec ces identifiants.
     *
     * @param triple Triplet RDF à encoder
     * @return Nouvel RDFAtom avec les éléments encodés
     * @throws IllegalArgumentException Si le triplet RDF est null
     */
    public RDFAtom encodeTriple(RDFAtom triple) {
        if (triple == null) {
            throw new IllegalArgumentException("Le triple RDF ne peut pas être null");
        }

        // Utiliser un StringBuilder pour optimiser les conversions
        StringBuilder buffer = new StringBuilder();
        
        // Encoder en batch
        int[] encodedValues = new int[3];
        encodedValues[0] = encode(triple.getTripleSubject().toString());
        encodedValues[1] = encode(triple.getTriplePredicate().toString());
        encodedValues[2] = encode(triple.getTripleObject().toString());

        // Créer les termes une seule fois
        Term[] terms = new Term[3];
        for (int i = 0; i < 3; i++) {
            buffer.setLength(0);
            buffer.append(encodedValues[i]);
            terms[i] = new ConstantImpl(buffer.toString());
        }

        return new RDFAtom(terms[0], terms[1], terms[2]);
    }

    /**
     * Décode un triplet RDF de manière optimisée.
     * Convertit chaque élément du triplet (sujet, prédicat, objet) en une chaîne de caractères
     * en utilisant la méthode decode. Crée ensuite un nouvel RDFAtom avec ces chaînes.
     *
     * @param encodedTriple Triplet RDF encodé à décoder
     * @return Nouvel RDFAtom avec les éléments décodés
     */
    public RDFAtom decodeTriple(RDFAtom encodedTriple) {
        // Utiliser un StringBuilder pour optimiser les conversions
        StringBuilder buffer = new StringBuilder();
        
        // Décoder en batch
        int[] decodedValues = new int[3];
        decodedValues[0] = Integer.parseInt(encodedTriple.getTripleSubject().toString());
        decodedValues[1] = Integer.parseInt(encodedTriple.getTriplePredicate().toString());
        decodedValues[2] = Integer.parseInt(encodedTriple.getTripleObject().toString());

        // Créer les termes une seule fois
        Term[] terms = new Term[3];
        for (int i = 0; i < 3; i++) {
            String decodedValue = decode(decodedValues[i]);
            terms[i] = new ConstantImpl(decodedValue);
        }

        return new RDFAtom(terms[0], terms[1], terms[2]);
    }

    /**
     * Retourne la taille du dictionnaire.
     * La taille correspond au nombre de chaînes de caractères uniques stockées dans le dictionnaire.
     *
     * @return Taille du dictionnaire
     */
    public int size() {
        return stringToId.size();
    }

    /**
     * Vide le cache.
     * Supprime toutes les entrées du cache LRU.
     */
    public void clearCache() {
        synchronized (cache) {
            cache.clear();
        }
    }

    /**
     * Retourne les statistiques du cache.
     * Fournit des informations sur la taille actuelle du cache, sa taille maximale et son utilisation.
     *
     * @return Statistiques du cache
     */
    public CacheStats getCacheStats() {
        return new CacheStats(cache.size(), DEFAULT_CACHE_SIZE);
    }

    /**
     * Classe pour les statistiques du cache.
     * Stocke et fournit des informations sur la taille actuelle du cache, sa taille maximale et son utilisation.
     */
    public static class CacheStats {
        private final int currentSize;
        private final int maxSize;

        public CacheStats(int currentSize, int maxSize) {
            this.currentSize = currentSize;
            this.maxSize = maxSize;
        }

        public int getCurrentSize() {
            return currentSize;
        }

        public int getMaxSize() {
            return maxSize;
        }

        /**
         * Calcule l'utilisation du cache en pourcentage.
         *
         * @return Utilisation du cache (pourcentage)
         */
        public double getUtilization() {
            return (double) currentSize / maxSize;
        }
    }
}