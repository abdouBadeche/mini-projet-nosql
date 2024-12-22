package qengine.model;

import fr.boreal.model.logicalElements.api.Atom;
import fr.boreal.model.logicalElements.api.Predicate;
import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.factory.impl.SameObjectPredicateFactory;
import fr.boreal.model.logicalElements.impl.AtomImpl;

import java.util.List;

/**
 * Représentation d'un triplet RDF sous forme d'atome.
 * Le prédicat est toujours "triple" et d'arité 3.
 */
public class RDFAtom extends AtomImpl {

    public static final Predicate TRIPLE_PREDICATE =
            SameObjectPredicateFactory.instance().createOrGetPredicate("triple", 3);
    /**
     * Constructeur d'un triplet RDF.
     *
     * @param a les trois termes du triplet RDF (sujet, prédicat, objet)
     * @throws IllegalArgumentException si le nombre de termes n'est pas égal à 3
     */
    public RDFAtom(Atom a) {
        super(TRIPLE_PREDICATE, a.getTerms());
        if (!a.getPredicate().equals(TRIPLE_PREDICATE)) {
            throw new IllegalArgumentException("Not a triple.");
        }
    }

    /**
     * Constructeur d'un triplet RDF.
     *
     * @param terms les trois termes du triplet RDF (sujet, prédicat, objet)
     * @throws IllegalArgumentException si le nombre de termes n'est pas égal à 3
     */
    public RDFAtom(List<Term> terms) {
        super(TRIPLE_PREDICATE, terms);
    }

    /**
     * Constructeur d'un triplet RDF.
     *
     * @param terms les trois termes du triplet RDF (sujet, prédicat, objet)
     * @throws IllegalArgumentException si le nombre de termes n'est pas égal à 3
     */
    public RDFAtom(Term... terms) {
        super(TRIPLE_PREDICATE, terms);
        validateTerms(terms);
    }

    /**
     * Valide que les termes respectent les contraintes de l'atome RDF.
     *
     * @param terms les termes à valider
     * @throws IllegalArgumentException si les contraintes ne sont pas respectées
     */
    private void validateTerms(Term[] terms) {
        if (terms.length != 3) {
            throw new IllegalArgumentException("Un RDFAtom doit avoir exactement 3 termes, mais " +
                    terms.length + " ont été fournis.");
        }
    }

    public Term getTripleSubject() {
        return this.getTerm(0);
    }

    public Term getTriplePredicate() {
        return this.getTerm(1);
    }

    public Term getTripleObject() {
        return this.getTerm(2);
    }

    /**
     * @return a string representation of this atom of the form p(t1,...,tn)
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('<');
        boolean first = true;
        for (Term t : this.getTerms()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(t.toString());
            first = false;
        }
        sb.append('>');
        return sb.toString();
    }
}
