package qengine.model;

import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.logicalElements.factory.api.TermFactory;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.query.api.FOQuery;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitaires pour la classe StarQuery.
 */
class StarQueryTest {

    private final TermFactory termFactory = SameObjectTermFactory.instance();

    @Test
    void testStarQueryConstructorValid() {
        Variable centralVariable = (Variable) termFactory.createOrGetVariable("?x");
        Term predicate1 = termFactory.createOrGetLiteral("http://example.org/predicate1");
        Term object1 = termFactory.createOrGetLiteral("http://example.org/object1");

        Term predicate2 = termFactory.createOrGetLiteral("http://example.org/predicate2");
        Term object2 = termFactory.createOrGetLiteral("http://example.org/object2");

        RDFAtom atom1 = new RDFAtom(centralVariable, predicate1, object1);
        RDFAtom atom2 = new RDFAtom(centralVariable, predicate2, object2);

        List<RDFAtom> rdfAtoms = List.of(atom1, atom2);
        Collection<Variable> answerVariables = List.of(centralVariable);

        StarQuery query = new StarQuery("Requête étoile valide", rdfAtoms, answerVariables);

        assertEquals("Requête étoile valide", query.getLabel());
        assertEquals(rdfAtoms, query.getRdfAtoms());
        assertEquals(answerVariables, query.getAnswerVariables());
        assertEquals(centralVariable, query.getCentralVariable());
    }

    @Test
    void testStarQueryConstructorInvalidNoSharedVariable() {
        Variable var1 = termFactory.createOrGetVariable("?x");
        Variable var2 = termFactory.createOrGetVariable("?y");
        Term predicate1 = termFactory.createOrGetLiteral("http://example.org/predicate1");
        Term object1 = termFactory.createOrGetLiteral("http://example.org/object1");

        Term predicate2 = termFactory.createOrGetLiteral("http://example.org/predicate2");
        Term object2 = termFactory.createOrGetLiteral("http://example.org/object2");

        RDFAtom atom1 = new RDFAtom(var1, predicate1, object1);
        RDFAtom atom2 = new RDFAtom(var2, predicate2, object2);

        List<RDFAtom> rdfAtoms = List.of(atom1, atom2);
        Collection<Variable> answerVariables = List.of(var1, var2);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new StarQuery("Requête étoile invalide", rdfAtoms, answerVariables);
        });

        assertTrue(exception.getMessage().contains("ne partagent pas une variable commune"));
    }

    @Test
    void testStarQueryConstructorInvalidAnswerVariableNotPresent() {
        Variable centralVariable = (Variable) termFactory.createOrGetVariable("?x");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        RDFAtom atom = new RDFAtom(centralVariable, predicate, object);

        List<RDFAtom> rdfAtoms = List.of(atom);

        Variable invalidVariable = (Variable) termFactory.createOrGetVariable("?y");
        Collection<Variable> answerVariables = List.of(invalidVariable);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new StarQuery("Requête étoile invalide", rdfAtoms, answerVariables);
        });

        assertTrue(exception.getMessage().contains("n'est pas présente dans les triplets RDF"));
    }

    @Test
    void testStarQueryEqualsAndHashCode() {
        Variable centralVariable = termFactory.createOrGetVariable("?x");
        Term predicate1 = termFactory.createOrGetLiteral("http://example.org/predicate1");
        Term object1 = termFactory.createOrGetLiteral("http://example.org/object1");

        Term predicate2 = termFactory.createOrGetLiteral("http://example.org/predicate2");
        Term object2 = termFactory.createOrGetLiteral("http://example.org/object2");

        RDFAtom atom1 = new RDFAtom(centralVariable, predicate1, object1);
        RDFAtom atom2 = new RDFAtom(centralVariable, predicate2, object2);

        List<RDFAtom> rdfAtoms = List.of(atom1, atom2);
        Collection<Variable> answerVariables = List.of(centralVariable);

        StarQuery query1 = new StarQuery("Requête étoile", rdfAtoms, answerVariables);
        StarQuery query2 = new StarQuery("Requête étoile", rdfAtoms, answerVariables);
        StarQuery query3 = new StarQuery("Requête étoile différente", rdfAtoms, answerVariables);

        // Vérification d'égalité
        assertEquals(query1, query2, "Deux requêtes identiques doivent être égales.");
        assertNotEquals(query1, query3, "Deux requêtes avec des labels différents ne doivent pas être égales.");

        // Vérification des hashCodes
        assertEquals(query1.hashCode(), query2.hashCode(), "Deux requêtes identiques doivent avoir le même hashCode.");
    }

    @Test
    void testStarQueryToString() {
        Variable centralVariable = termFactory.createOrGetVariable("?x");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        RDFAtom atom = new RDFAtom(centralVariable, predicate, object);
        List<RDFAtom> rdfAtoms = List.of(atom);
        Collection<Variable> answerVariables = List.of(centralVariable);

        StarQuery query = new StarQuery("Requête étoile", rdfAtoms, answerVariables);

        String toString = query.toString();

        // Vérifications du contenu de la représentation texte
        assertTrue(toString.contains("Requête étoile"), "Le toString doit contenir le label de la requête.");
        assertTrue(toString.contains(centralVariable.toString()), "Le toString doit contenir la variable centrale.");
        assertTrue(toString.contains(predicate.toString()), "Le toString doit contenir les termes du triplet.");
        assertTrue(toString.contains(object.toString()), "Le toString doit contenir les termes du triplet.");
    }

    @Test
    void testStarQueryEquals() {
        Variable centralVariable = termFactory.createOrGetVariable("?x");
        Term predicate1 = termFactory.createOrGetLiteral("http://example.org/predicate1");
        Term object1 = termFactory.createOrGetLiteral("http://example.org/object1");

        Term predicate2 = termFactory.createOrGetLiteral("http://example.org/predicate2");
        Term object2 = termFactory.createOrGetLiteral("http://example.org/object2");

        RDFAtom atom1 = new RDFAtom(centralVariable, predicate1, object1);
        RDFAtom atom2 = new RDFAtom(centralVariable, predicate2, object2);

        List<RDFAtom> rdfAtoms = List.of(atom1, atom2);
        Collection<Variable> answerVariables = List.of(centralVariable);

        // Création des requêtes
        StarQuery query = new StarQuery("Requête étoile", rdfAtoms, answerVariables);
        StarQuery identicalQuery = new StarQuery("Requête étoile", rdfAtoms, answerVariables);

        // Cas où les rdfAtoms diffèrent
        RDFAtom atom3 = new RDFAtom(centralVariable, predicate1, termFactory.createOrGetLiteral("http://example.org/object3"));
        List<RDFAtom> differentRdfAtoms = List.of(atom1, atom3);
        StarQuery queryWithDifferentRdfAtoms = new StarQuery("Requête étoile", differentRdfAtoms, answerVariables);

        // Comparaison avec une requête ayant des answerVariables différentes
        Variable differentVariable = termFactory.createOrGetVariable("?y"); // Nouvelle variable réponse valide
        RDFAtom atomWithDifferentVariable = new RDFAtom(differentVariable, predicate1, object1);
        RDFAtom atomWithDifferentVariable2 = new RDFAtom(differentVariable, predicate2, object2);
        List<RDFAtom> rdfAtomsWithDifferentVariable = List.of(atomWithDifferentVariable, atomWithDifferentVariable2);
        Collection<Variable> differentAnswerVariables = List.of(differentVariable);
        StarQuery queryWithDifferentAnswerVariables = new StarQuery("Requête étoile différente", rdfAtomsWithDifferentVariable, differentAnswerVariables);

        // Comparaison avec la même requête sans variable réponse
        StarQuery queryWithNoAnswerVariables = new StarQuery("Requête étoile", rdfAtoms, List.of());

        // Assertions
        assertEquals(query, query, "Une requête doit être égale à elle-même.");
        assertEquals(query, identicalQuery, "Deux requêtes identiques doivent être égales.");
        assertNotEquals(query, null, "Une requête ne doit pas être égale à null.");
        assertNotEquals(query, "Autre type", "Une requête ne doit pas être égale à un objet d'une classe différente.");
        assertNotEquals(query, queryWithDifferentRdfAtoms, "Deux requêtes avec des rdfAtoms différents ne doivent pas être égales.");
        assertNotEquals(query, queryWithDifferentAnswerVariables, "Deux requêtes avec des answerVariables différentes ne doivent pas être égales.");
        assertNotEquals(query, queryWithNoAnswerVariables, "Deux requêtes avec des answerVariables différentes ne doivent pas être égales.");
    }

    @Test
    void testAsFOQuery() {
        Variable centralVariable = termFactory.createOrGetVariable("?v0");
        Term predicate1 = termFactory.createOrGetLiteral("http://example.org/predicate1");
        Term object1 = termFactory.createOrGetLiteral("http://example.org/object1");

        Term predicate2 = termFactory.createOrGetLiteral("http://example.org/predicate2");
        Term object2 = termFactory.createOrGetLiteral("http://example.org/object2");

        // Création des RDFAtoms
        RDFAtom atom1 = new RDFAtom(centralVariable, predicate1, object1);
        RDFAtom atom2 = new RDFAtom(centralVariable, predicate2, object2);

        List<RDFAtom> rdfAtoms = List.of(atom1, atom2);
        Collection<Variable> answerVariables = List.of(centralVariable);

        // Création de la requête étoile
        StarQuery starQuery = new StarQuery("Requête étoile", rdfAtoms, answerVariables);

        // Conversion en FOQuery
        FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery();

        // Vérifications
        assertNotNull(foQuery, "La requête FOQuery ne doit pas être null.");
        assertEquals("Requête étoile", foQuery.getLabel(), "Le label de la requête FOQuery doit correspondre au label de la requête étoile.");

        // Vérifier que la conjonction contient les RDFAtoms correctement
        FOFormulaConjunction conjunction = foQuery.getFormula();
        assertEquals(2, conjunction.asAtomSet().size(), "La conjonction doit contenir deux atomes.");

        // Vérification des termes dans la conjonction
        assertTrue(conjunction.asAtomSet().contains(atom1), "La conjonction doit contenir atom1.");
        assertTrue(conjunction.asAtomSet().contains(atom2), "La conjonction doit contenir atom2.");

        // Vérification des variables de réponse
        assertEquals(answerVariables, foQuery.getAnswerVariables(), "Les variables de réponse doivent être les mêmes que celles de la requête étoile.");
    }

}
