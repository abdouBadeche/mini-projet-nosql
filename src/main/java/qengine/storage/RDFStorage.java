package qengine.storage;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import fr.boreal.model.logicalElements.api.Atom;
import fr.boreal.model.logicalElements.api.Substitution;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

/**
 * Contrat pour un système de stockage de données RDF
 */
public interface RDFStorage {

    /**
     * Ajoute un RDFAtom dans le store.
     *
     * @param a le RDFAtom à ajouter
     * @return true si le RDFAtom a été ajouté avec succès, false s'il est déjà présent
     */
    boolean add(RDFAtom a);

    /**
     * @param a atom
     * @return un itérateur de substitutions correspondant aux match des atomes
     *          (i.e., sur quels termes s'envoient les variables)
     */
    Iterator<Substitution> match(RDFAtom a);


    /**
     * @param q star query
     * @return an itérateur de subsitutions décrivrant les réponses à la requete
     */
    Iterator<Substitution> match(StarQuery q);

    /**
     * Retourne le nombre d'atomes dans le Store.
     *
     * @return le nombre d'atomes
     */
    long size();

    /**
     * Retourne une collections contenant tous les atomes du store.
     * Utile pour les tests unitaires.
     *
     * @return une collection d'atomes
     */
    Collection<Atom> getAtoms();

    /**
     * Ajoute des RDFAtom dans le store.
     *
     * @param atoms les RDFAtom à ajouter
     * @return true si au moins un RDFAtom a été ajouté, false s'ils sont tous déjà présents
     */
    default boolean addAll(Stream<RDFAtom> atoms) {
        return atoms.map(this::add).reduce(Boolean::logicalOr).orElse(false);
    }

    /**
     * Ajoute des RDFAtom dans le store.
     *
     * @param atoms les RDFAtom à ajouter
     * @return true si au moins un RDFAtom a été ajouté, false s'ils sont tous déjà présents
     */
    default boolean addAll(Collection<RDFAtom> atoms) {
        return this.addAll(atoms.stream());
    }
}
