package qengine.model;

import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.formula.factory.FOFormulaFactory;
import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.query.factory.FOQueryFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Représentation d'une requête en étoile.
 * Une requête en étoile est composée d'une collection de triplets RDF
 * qui partagent une seule variable centrale.
 */
public class StarQuery implements Query {

    // Label de la requête
    private final String label;

    // Collection des triplets RDF (les atomes de la requête)
    private final List<RDFAtom> rdfAtoms;

    // variables réponses
    private final Collection<Variable> answerVariables;

    // Variable centrale de la requête en étoile
    private final Variable centralVariable;

    /**
     * Constructeur pour une requête en étoile.
     *
     * @param label           le label de la requête
     * @param rdfAtoms        la collection des triplets RDF
     * @param answerVariables les variables réponses
     * @throws NullPointerException     si l'un des paramètres est null
     * @throws IllegalArgumentException si les atomes RDF ne forment pas une requête en étoile
     */
    public StarQuery(String label, List<RDFAtom> rdfAtoms, Collection<Variable> answerVariables) {
        this.label = Objects.requireNonNull(label, "Le label ne peut pas être null.");
        this.rdfAtoms = Objects.requireNonNull(rdfAtoms, "Les triplets RDF ne peuvent pas être null.");
        this.answerVariables = Objects.requireNonNull(answerVariables, "Les variables réponses ne peuvent pas être null.");

        // Déterminer la variable centrale
        this.centralVariable = determineCentralVariable(rdfAtoms);

        // Vérifier que toutes les variables réponses sont valides
        validateAnswerVariables(answerVariables, rdfAtoms);
    }

    /**
     * Détermine la variable centrale partagée par tous les triplets RDF.
     *
     * @param rdfAtoms la collection de triplets RDF
     * @return la variable centrale
     * @throws IllegalArgumentException si les triplets ne partagent pas une variable commune
     */
    private Variable determineCentralVariable(Collection<RDFAtom> rdfAtoms) {
        Set<Variable> sharedVariables = rdfAtoms.stream()
                .flatMap(atom -> Arrays.stream(atom.getTerms()))
                .filter(term -> term instanceof Variable)
                .map(term -> (Variable) term)
                .collect(Collectors.toSet());

        // Vérifier qu'une seule variable est partagée
        for (Variable candidate : sharedVariables) {
            if (rdfAtoms.stream()
                    .allMatch(atom -> Arrays.asList(atom.getTerms()).contains(candidate))) {
                return candidate;
            }
        }

        throw new IllegalArgumentException("Les triplets RDF ne partagent pas une variable commune.");
    }

    /**
     * Valide que toutes les variables réponses appartiennent aux triplets RDF.
     *
     * @param answerVariables les variables réponses
     * @param rdfAtoms        la collection de triplets RDF
     * @throws IllegalArgumentException si une variable de réponse n'est pas présente
     */
    private void validateAnswerVariables(Collection<Variable> answerVariables, Collection<RDFAtom> rdfAtoms) {
        Set<Term> allTerms = rdfAtoms.stream()
                .flatMap(atom -> Arrays.stream(atom.getTerms()))
                .collect(Collectors.toSet());

        for (Variable answerVariable : answerVariables) {
            if (!allTerms.contains(answerVariable)) {
                throw new IllegalArgumentException("La variable réponse " + answerVariable +
                        " n'est pas présente dans les triplets RDF.");
            }
        }
    }

    /**
     * Retourne le label de la requête.
     *
     * @return le label
     */
    @Override
    public String getLabel() {
        return label;
    }

    /**
     * Retourne les variables réponses.
     *
     * @return les variables réponses
     */
    @Override
    public Collection<Variable> getAnswerVariables() {
        return answerVariables;
    }

    /**
     * Retourne la variable centrale de la requête en étoile.
     *
     * @return la variable centrale
     */
    public Variable getCentralVariable() {
        return centralVariable;
    }

    /**
     * Retourne la collection des triplets RDF.
     *
     * @return la collection des triplets RDF
     */
    public List<RDFAtom> getRdfAtoms() {
        return rdfAtoms;
    }

    /**
     * Convertit la requete en étoile en requete pour Integraal
     *
     * @return FOQuery
     */
    public FOQuery<FOFormulaConjunction> asFOQuery() {
        FOFormulaConjunction conjunction = FOFormulaFactory.instance().createOrGetConjunction(this.rdfAtoms);
        return FOQueryFactory.instance().createOrGetQuery(this.label, conjunction, this.answerVariables);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarQuery that = (StarQuery) o;
        return label.equals(that.label) &&
                rdfAtoms.equals(that.rdfAtoms) &&
                answerVariables.equals(that.answerVariables) &&
                centralVariable.equals(that.centralVariable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, rdfAtoms, answerVariables, centralVariable);
    }

    @Override
    public String toString() {
        return "StarQuery{" +
                "label='" + label + '\'' +
                ",\n\t rdfAtoms=" + rdfAtoms +
                ",\n\t answerVariables=" + answerVariables +
                ",\n\t centralVariable=" + centralVariable +
                '}';
    }
}
