package qengine.model;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import fr.lirmm.graphik.graal.api.core.Atom;
import fr.lirmm.graphik.graal.api.core.Constant;
import fr.lirmm.graphik.graal.api.core.Literal;
import fr.lirmm.graphik.graal.api.core.Predicate;
import fr.lirmm.graphik.graal.api.core.Term;
import fr.lirmm.graphik.graal.api.core.Term.Type;
import fr.lirmm.graphik.graal.api.core.Variable;

/**
 * Une implémentation concrète de l'interface Atom.
 * Un atome est généralement représenté par un prédicat et une liste de termes.
 */
public class DefaultAtom implements Atom {
    /**
     * Le prédicat de l'atome.
     */
    private Predicate predicate;

    /**
     * La liste des termes de l'atome.
     */
    private List<Term> terms;

    /**
     * Constructeur de la classe DefaultAtom.
     *
     * @param predicate Le prédicat de l'atome.
     * @param terms     La liste des termes de l'atome.
     */
    public DefaultAtom(Predicate predicate, List<Term> terms) {
        this.predicate = predicate;
        this.terms = terms;
    }

    /**
     * Définit le prédicat de l'atome.
     *
     * @param predicate Le nouveau prédicat de l'atome.
     */
    @Override
    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }

    /**
     * Retourne le prédicat de l'atome.
     *
     * @return Le prédicat de l'atome.
     */
    @Override
    public Predicate getPredicate() {
        return this.predicate;
    }

    /**
     * Définit un terme spécifique de l'atome par son index.
     *
     * @param index L'index du terme à modifier.
     * @param term  Le nouveau terme à placer à l'index spécifié.
     */
    @Override
    public void setTerm(int index, Term term) {
        this.terms.set(index, term);
    }

    /**
     * Retourne un terme spécifique de l'atome par son index.
     *
     * @param index L'index du terme à retourner.
     * @return Le terme à l'index spécifié.
     */
    @Override
    public Term getTerm(int index) {
        return this.terms.get(index);
    }

    /**
     * Retourne la liste complète des termes de l'atome.
     *
     * @return La liste des termes de l'atome.
     */
    @Override
    public List<Term> getTerms() {
        return this.terms;
    }

    /**
     * Retourne un itérateur sur les termes de l'atome.
     *
     * @return Un itérateur sur les termes de l'atome.
     */
    @Override
    public Iterator<Term> iterator() {
        return this.terms.iterator();
    }

    /**
     * Retourne l'ensemble des variables présentes dans les termes de l'atome.
     *
     * @return L'ensemble des variables présentes dans les termes de l'atome.
     */
    @Override
    public Set<Variable> getVariables() {
        // Implémentation spécifique selon les types de termes
        // Par exemple, on pourrait parcourir la liste des termes et ajouter les variables dans un ensemble
        return null;
    }

    /**
     * Retourne l'ensemble des constantes présentes dans les termes de l'atome.
     *
     * @return L'ensemble des constantes présentes dans les termes de l'atome.
     */
    @Override
    public Set<Constant> getConstants() {
        // Implémentation spécifique selon les types de termes
        // Par exemple, on pourrait parcourir la liste des termes et ajouter les constantes dans un ensemble
        return null;
    }

    /**
     * Retourne l'ensemble des littéraux présents dans les termes de l'atome.
     *
     * @return L'ensemble des littéraux présents dans les termes de l'atome.
     */
    @Override
    public Set<Literal> getLiterals() {
        // Implémentation spécifique selon les types de termes
        // Par exemple, on pourrait parcourir la liste des termes et ajouter les littéraux dans un ensemble
        return null;
    }

    /**
     * Compare cet atome avec un autre atome.
     *
     * @param o L'autre atome à comparer.
     * @return Un entier négatif, zéro ou positif si cet atome est respectivement inférieur, égal ou supérieur à l'atome spécifié.
     */
    @Override
    public int compareTo(Atom o) {
        // Implémentation de la comparaison entre deux atomes
        // Par exemple, on pourrait comparer les prédicats puis les termes un par un
        return 0;
    }

    /**
     * Ajoute une représentation textuelle de cet atome dans le StringBuilder fourni.
     *
     * @param sb Le StringBuilder dans lequel ajouter la représentation textuelle de l'atome.
     */
    @Override
    public void appendTo(StringBuilder sb) {
        // Implémentation de l'ajout de la représentation textuelle de l'atome dans le StringBuilder
        // Par exemple, on pourrait ajouter le prédicat puis les termes entre parenthèses
    }

    /**
     * Retourne l'index du premier terme spécifié dans la liste des termes de l'atome.
     *
     * @param t Le terme dont on cherche l'index.
     * @return L'index du premier terme spécifié, ou -1 s'il n'est pas présent.
     */
    @Override
    public int indexOf(Term t) {
        // Implémentation de la recherche de l'index du terme spécifié
        return this.terms.indexOf(t);
    }

    /**
     * Retourne un tableau des index de tous les termes spécifiés dans la liste des termes de l'atome.
     *
     * @param term Le terme dont on cherche les index.
     * @return Un tableau des index de tous les termes spécifiés, ou un tableau vide s'il n'est pas présent.
     */
    @Override
    public int[] indexesOf(Term term) {
        // Implémentation de la recherche de tous les index du terme spécifié
        return null;
    }

    /**
     * Vérifie si l'atome contient un terme spécifique.
     *
     * @param t Le terme à rechercher.
     * @return Vrai si l'atome contient le terme spécifié, faux sinon.
     */
    @Override
    public boolean contains(Term t) {
        // Implémentation de la vérification de la présence du terme spécifié
        return this.terms.contains(t);
    }

    /**
     * Retourne une collection des termes de l'atome d'un type spécifique.
     *
     * @param type Le type des termes à retourner.
     * @return Une collection des termes de l'atome d'un type spécifique.
     */
    @Override
    public Collection<Term> getTerms(Type type) {
        // Implémentation de la récupération des termes d'un type spécifique
        // Par exemple, on pourrait filtrer la liste des termes selon le type spécifié
        return null;
    }
}