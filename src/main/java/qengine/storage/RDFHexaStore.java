package qengine.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import fr.boreal.model.logicalElements.api.Atom;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.logicalElements.impl.ConstantImpl;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import qengine.dictionary.Dictionary;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

/**
 * Classe RDFHexaStore qui implémente l'interface RDFStorage.
 * Utilise six index pour stocker les triplets RDF de manière efficace.
 * Les index permettent des recherches rapides en fonction des différents ordres de sujet, prédicat et objet.
 */
public class RDFHexaStore implements RDFStorage {
    // Index SPO : Sujet -> Prédicat -> Objet
    private final Map<String, Map<String, Set<String>>> spo;
    // Index SOP : Sujet -> Objet -> Prédicat
    private final Map<String, Map<String, Set<String>>> sop;
    // Index PSO : Prédicat -> Sujet -> Objet
    private final Map<String, Map<String, Set<String>>> pso;
    // Index POS : Prédicat -> Objet -> Sujet
    private final Map<String, Map<String, Set<String>>> pos;
    // Index OSP : Objet -> Sujet -> Prédicat
    private final Map<String, Map<String, Set<String>>> osp;
    // Index OPS : Objet -> Prédicat -> Sujet
    private final Map<String, Map<String, Set<String>>> ops;

    // Dictionnaire pour encoder et décoder les chaînes de caractères en identifiants uniques
    private final Dictionary dictionary;
    // Taille du stockage RDF
    private long size;

    /**
     * Constructeur de la classe RDFHexaStore.
     * Initialise les six index et le dictionnaire.
     *
     * @param dictionary Le dictionnaire utilisé pour l'encodage et le décodage des termes.
     * @throws NullPointerException Levée si le dictionnaire est null.
     */
    public RDFHexaStore(Dictionary dictionary) {
        this.dictionary = Objects.requireNonNull(dictionary);
        this.spo = new HashMap<>();
        this.sop = new HashMap<>();
        this.pso = new HashMap<>();
        this.pos = new HashMap<>();
        this.osp = new HashMap<>();
        this.ops = new HashMap<>();
        this.size = 0;
    }

    /**
     * Ajoute un atome RDF au stockage.
     * Encode les termes du triplet et les ajoute dans les six index.
     *
     * @param atom L'atome RDF à ajouter.
     * @return true si l'atome a été ajouté, false s'il était déjà présent.
     * @throws IllegalArgumentException Levée si l'atome est null.
     */
    @Override
    public boolean add(RDFAtom atom) {
        if (atom == null) {
            throw new IllegalArgumentException("L'atome ne peut pas être null");
        }

        // Encode les termes du triplet RDF
        String s = String.valueOf(dictionary.encode(atom.getTripleSubject().toString()));
        String p = String.valueOf(dictionary.encode(atom.getTriplePredicate().toString()));
        String o = String.valueOf(dictionary.encode(atom.getTripleObject().toString()));

        // Ajoute le triplet dans les six index
        boolean added = addToIndex(spo, s, p, o);
        added |= addToIndex(sop, s, o, p);
        added |= addToIndex(pso, p, s, o);
        added |= addToIndex(pos, p, o, s);
        added |= addToIndex(osp, o, s, p);
        added |= addToIndex(ops, o, p, s);

        // Incrémente la taille si le triplet a été ajouté
        if (added) {
            size++;
        }
        return added;
    }

    /**
     * Ajoute un triplet dans l'index spécifié.
     * Utilise computeIfAbsent pour ajouter les entrées si elles n'existent pas.
     *
     * @param index  L'index dans lequel ajouter le triplet.
     * @param first  La première clé de l'index (sujet, prédicat ou objet).
     * @param second La deuxième clé de l'index (prédicat, objet ou sujet).
     * @param third  La troisième clé de l'index (objet, sujet ou prédicat).
     * @return true si le triplet a été ajouté, false s'il était déjà présent.
     */
    private boolean addToIndex(Map<String, Map<String, Set<String>>> index, 
                             String first, String second, String third) {
        return index.computeIfAbsent(first, k -> new HashMap<>())
                   .computeIfAbsent(second, k -> new HashSet<>())
                   .add(third);
    }

    /**
     * Optimise l'ordre des atomes pour une requête étoilée.
     * Trie les atomes en fonction de leur sélectivité.
     *
     * @param atoms La liste d'atomes RDF à optimiser.
     * @return Une nouvelle liste d'atomes RDF triée par sélectivité.
     */
    private List<RDFAtom> optimizeQueryOrder(List<RDFAtom> atoms) {
        return atoms.stream()
            .sorted(Comparator.comparingInt(this::estimateSelectivity))
            .collect(Collectors.toList());
    }

    /**
     * Estime la sélectivité d'un atome RDF.
     * Les atomes avec des constantes sont plus sélectifs que ceux avec des variables.
     * Les prédicats rares sont également plus sélectifs.
     *
     * @param atom L'atome RDF dont la sélectivité est estimée.
     * @return Un score de sélectivité (négatif pour que les plus sélectifs soient en premier).
     */
    private int estimateSelectivity(RDFAtom atom) {
        Term s = atom.getTripleSubject();
        Term p = atom.getTriplePredicate();
        Term o = atom.getTripleObject();
        
        // Plus le score est bas, plus l'atome est sélectif
        int score = 0;
        
        // Les constantes sont plus sélectives que les variables
        if (!(s instanceof Variable)) score += 1;
        if (!(p instanceof Variable)) score += 1;
        if (!(o instanceof Variable)) score += 1;
        
        // Bonus pour les prédicats car souvent plus sélectifs
        if (!(p instanceof Variable)) {
            String predicate = p.toString();
            int predCount = getPredicateCount(predicate);
            score -= (predCount < size() / 10) ? 2 : 0; // Bonus pour les prédicats rares
        }
        
        return -score; // Négatif pour que les plus sélectifs soient en premier
    }

    /**
     * Retourne le nombre d'occurrences d'un prédicat dans le stockage RDF.
     *
     * @param predicate Le prédicat dont le nombre d'occurrences est estimé.
     * @return Le nombre d'occurrences du prédicat.
     */
    private int getPredicateCount(String predicate) {
        String encodedPred = String.valueOf(dictionary.encode(predicate));
        Map<String, Set<String>> predicateIndex = pso.get(encodedPred);
        return predicateIndex != null ? predicateIndex.values().stream()
                .mapToInt(Set::size).sum() : 0;
    }

    /**
     * Recherche les substitutions qui correspondent au motif RDF spécifié.
     * Utilise les index pour trouver les triplets correspondants.
     *
     * @param pattern Le motif RDF à rechercher.
     * @return Un itérateur sur les substitutions correspondantes.
     * @throws IllegalArgumentException Levée si le motif est null.
     */
    @Override
    public Iterator<Substitution> match(RDFAtom pattern) {
        Set<Substitution> results = new HashSet<>();

        // Récupère les termes du motif RDF
        Term s = pattern.getTripleSubject();
        Term p = pattern.getTriplePredicate();
        Term o = pattern.getTripleObject();

        // Encode les termes constants du motif RDF
        String encodedS = s instanceof Variable ? null : String.valueOf(dictionary.encode(s.toString()));
        String encodedP = p instanceof Variable ? null : String.valueOf(dictionary.encode(p.toString()));
        String encodedO = o instanceof Variable ? null : String.valueOf(dictionary.encode(o.toString()));

        // Utiliser l'index SPO pour la recherche si le sujet est connu
        if (encodedS != null) {
            Map<String, Set<String>> predicates = spo.get(encodedS);
            if (predicates != null) {
                // Si le prédicat est connu
                if (encodedP != null) {
                    Set<String> objects = predicates.get(encodedP);
                    if (objects != null) {
                        for (String obj : objects) {
                            // Si l'objet est connu ou variable
                            if (encodedO == null || obj.equals(encodedO)) {
                                Substitution sub = new SubstitutionImpl();
                                // Ajoute les substitutions pour les variables
                                if (s instanceof Variable) {
                                    sub.add((Variable) s, new ConstantImpl(dictionary.decode(Integer.parseInt(encodedS))));
                                }
                                if (p instanceof Variable) {
                                    sub.add((Variable) p, new ConstantImpl(dictionary.decode(Integer.parseInt(encodedP))));
                                }
                                if (o instanceof Variable) {
                                    sub.add((Variable) o, new ConstantImpl(dictionary.decode(Integer.parseInt(obj))));
                                }
                                results.add(sub);
                            }
                        }
                    }
                } else {
                    // Si le prédicat est variable
                    for (Map.Entry<String, Set<String>> predEntry : predicates.entrySet()) {
                        for (String obj : predEntry.getValue()) {
                            // Si l'objet est connu ou variable
                            if (encodedO == null || obj.equals(encodedO)) {
                                Substitution sub = new SubstitutionImpl();
                                // Ajoute les substitutions pour les variables
                                if (s instanceof Variable) {
                                    sub.add((Variable) s, new ConstantImpl(dictionary.decode(Integer.parseInt(encodedS))));
                                }
                                if (p instanceof Variable) {
                                    sub.add((Variable) p, new ConstantImpl(dictionary.decode(Integer.parseInt(predEntry.getKey()))));
                                }
                                if (o instanceof Variable) {
                                    sub.add((Variable) o, new ConstantImpl(dictionary.decode(Integer.parseInt(obj))));
                                }
                                results.add(sub);
                            }
                        }
                    }
                }
            }
        } else {
            // Si le sujet est variable, parcourir tous les sujets
            for (Map.Entry<String, Map<String, Set<String>>> subjectEntry : spo.entrySet()) {
                String subject = subjectEntry.getKey();
                for (Map.Entry<String, Set<String>> predicateEntry : subjectEntry.getValue().entrySet()) {
                    String predicate = predicateEntry.getKey();
                    // Si le prédicat est connu ou variable
                    if (encodedP == null || predicate.equals(encodedP)) {
                        for (String object : predicateEntry.getValue()) {
                            // Si l'objet est connu ou variable
                            if (encodedO == null || object.equals(encodedO)) {
                                Substitution sub = new SubstitutionImpl();
                                // Ajoute les substitutions pour les variables
                                if (s instanceof Variable) {
                                    sub.add((Variable) s, new ConstantImpl(dictionary.decode(Integer.parseInt(subject))));
                                }
                                if (p instanceof Variable) {
                                    sub.add((Variable) p, new ConstantImpl(dictionary.decode(Integer.parseInt(predicate))));
                                }
                                if (o instanceof Variable) {
                                    sub.add((Variable) o, new ConstantImpl(dictionary.decode(Integer.parseInt(object))));
                                }
                                results.add(sub);
                            }
                        }
                    }
                }
            }
        }

        return results.iterator();
    }

    /**
     * Recherche les substitutions qui correspondent à la requête étoilée spécifiée.
     * Utilise la méthode match pour chaque atome de la requête et effectue des jointures.
     *
     * @param query La requête étoilée à rechercher.
     * @return Un itérateur sur les substitutions correspondantes.
     * @throws IllegalArgumentException Levée si la requête est null.
     */
    @Override
    public Iterator<Substitution> match(StarQuery query) {
        if (query == null) {
            throw new IllegalArgumentException("La requête ne peut pas être null");
        }

        List<RDFAtom> atoms = query.getRdfAtoms();
        if (atoms.isEmpty()) {
            return Collections.emptyIterator();
        }

        // Optimiser l'ordre des atomes
        atoms = optimizeQueryOrder(atoms);

        // Commencer avec les résultats du premier atome (le plus sélectif)
        Iterator<Substitution> results = match(atoms.get(0));
        Set<Substitution> currentResults = new HashSet<>();
        while (results.hasNext()) {
            currentResults.add(results.next());
        }

        // Joindre avec les autres atomes de manière optimisée
        for (int i = 1; i < atoms.size(); i++) {
            Set<Substitution> newResults = new HashSet<>();
            RDFAtom currentAtom = atoms.get(i);

            for (Substitution currentSub : currentResults) {
                RDFAtom groundedAtom = substituteAtom(currentAtom, currentSub);
                Iterator<Substitution> matches = match(groundedAtom);

                while (matches.hasNext()) {
                    Substitution newSub = matches.next();
                    Optional<Substitution> merged = mergeSubstitutions(currentSub, newSub);
                    merged.ifPresent(newResults::add);
                }
            }

            if (newResults.isEmpty()) {
                return Collections.emptyIterator();
            }
            currentResults = newResults;
        }

        // Filtrer les résultats pour ne garder que les variables de réponse
        return filterResults(currentResults, query.getAnswerVariables()).iterator();
    }

    /**
     * Fusionne deux substitutions.
     * Retourne une substitution fusionnée si possible, sinon une option vide.
     *
     * @param sub1 La première substitution.
     * @param sub2 La deuxième substitution.
     * @return Une option contenant la substitution fusionnée, ou vide si la fusion échoue.
     */
    private Optional<Substitution> mergeSubstitutions(Substitution sub1, Substitution sub2) {
        try {
            return sub1.merged(sub2);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Filtre les résultats pour ne garder que les variables de réponse spécifiées.
     *
     * @param results Les résultats à filtrer.
     * @param answerVars Les variables de réponse à conserver.
     * @return Un ensemble de substitutions filtrées.
     */
    private Set<Substitution> filterResults(Set<Substitution> results, Collection<Variable> answerVars) {
        return results.stream()
            .map(sub -> {
                Substitution filtered = new SubstitutionImpl();
                for (Variable var : answerVars) {
                    Term image = sub.createImageOf(var);
                    if (image != null) {
                        filtered.add(var, image);
                    }
                }
                return filtered;
            })
            .collect(Collectors.toSet());
    }

    /**
     * Substitue les termes de l'atome RDF si possible en utilisant la substitution donnée.
     *
     * @param atom L'atome RDF à substituer.
     * @param sub  La substitution à appliquer.
     * @return Un nouvel atome RDF avec les termes substitués si possible.
     */
    private RDFAtom substituteAtom(RDFAtom atom, Substitution sub) {
        Term[] terms = new Term[3];
        terms[0] = substituteTermIfPossible(atom.getTripleSubject(), sub);
        terms[1] = substituteTermIfPossible(atom.getTriplePredicate(), sub);
        terms[2] = substituteTermIfPossible(atom.getTripleObject(), sub);
        return new RDFAtom(terms);
    }

    /**
     * Substitue un terme si c'est une variable et qu'il a une image dans la substitution.
     *
     * @param term Le terme à substituer.
     * @param sub  La substitution à appliquer.
     * @return Le terme substitué si possible, sinon le terme original.
     */
    private Term substituteTermIfPossible(Term term, Substitution sub) {
        if (term instanceof Variable) {
            Term substituted = sub.createImageOf(term);
            return substituted != null ? substituted : term;
        }
        return term;
    }

    /**
     * Retourne la taille du stockage RDF.
     *
     * @return La taille du stockage RDF.
     */
    @Override
    public long size() {
        return size;
    }

    /**
     * Retourne tous les atomes RDF stockés.
     *
     * @return Une collection d'atomes RDF.
     */
    @Override
    public Collection<Atom> getAtoms() {
        Set<Atom> atoms = new HashSet<>();
        for (Map.Entry<String, Map<String, Set<String>>> subjectEntry : spo.entrySet()) {
            String subject = subjectEntry.getKey();
            for (Map.Entry<String, Set<String>> predicateEntry : subjectEntry.getValue().entrySet()) {
                String predicate = predicateEntry.getKey();
                for (String object : predicateEntry.getValue()) {
                    // Crée un nouvel atome RDF avec les termes décodés
                    atoms.add(new RDFAtom(
                        new ConstantImpl(dictionary.decode(Integer.parseInt(subject))),
                        new ConstantImpl(dictionary.decode(Integer.parseInt(predicate))),
                        new ConstantImpl(dictionary.decode(Integer.parseInt(object)))
                    ));
                }
            }
        }
        return atoms;
    }
}