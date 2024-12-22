package qengine.model;

import fr.boreal.model.logicalElements.api.Atom;
import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.factory.api.TermFactory;
import fr.boreal.model.logicalElements.factory.impl.SameObjectPredicateFactory;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.AtomImpl;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitaires pour la classe RDFAtom.
 */
class RDFAtomTest {

    private final TermFactory termFactory = SameObjectTermFactory.instance();

    @Test
    void testRDFAtomConstructorWithValidTerms() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'instance
        RDFAtom rdfAtom = new RDFAtom(subject, predicate, object);

        // Assertions
        assertEquals(3, rdfAtom.getTerms().length, "L'atome RDF doit contenir exactement 3 termes.");
        assertEquals("triple", rdfAtom.getPredicate().label(), "Le prédicat doit être 'triple'.");
        assertEquals(3, rdfAtom.getPredicate().arity(), "L'arité du prédicat doit être 3.");
    }

    @Test
    void testRDFAtomConstructorWithInvalidArity() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");

        // Vérification qu'une exception est levée pour une arité incorrecte
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new RDFAtom(subject, predicate); // Seulement 2 termes
        });

        assertTrue(exception.getMessage().contains("exactement 3 termes"), "Le message d'erreur doit indiquer une arité incorrecte.");
    }

    @Test
    void testRDFAtomEquality() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de deux instances identiques
        RDFAtom rdfAtom1 = new RDFAtom(subject, predicate, object);
        RDFAtom rdfAtom2 = new RDFAtom(subject, predicate, object);

        // Assertions
        assertEquals(rdfAtom1, rdfAtom2, "Deux atomes RDF avec les mêmes termes doivent être égaux.");
        assertEquals(rdfAtom1.hashCode(), rdfAtom2.hashCode(), "Le hashcode de deux atomes RDF identiques doit être le même.");
    }

    @Test
    void testRDFAtomToString() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'atome RDF
        RDFAtom rdfAtom = new RDFAtom(subject, predicate, object);

        // Vérification du toString
        String expected = "<http://example.org/subject, http://example.org/predicate, http://example.org/object>";
        assertEquals(expected, rdfAtom.toString(), "La méthode toString doit produire une représentation correcte de l'atome RDF.");
    }

    @Test
    void testRDFAtomContainsTerm() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'atome RDF
        RDFAtom rdfAtom = new RDFAtom(subject, predicate, object);

        // Assertions
        assertTrue(rdfAtom.contains(subject), "L'atome RDF doit contenir le terme 'subject'.");
        assertTrue(rdfAtom.contains(predicate), "L'atome RDF doit contenir le terme 'predicate'.");
        assertTrue(rdfAtom.contains(object), "L'atome RDF doit contenir le terme 'object'.");

        Term otherTerm = termFactory.createOrGetLiteral("http://example.org/other");
        assertFalse(rdfAtom.contains(otherTerm), "L'atome RDF ne doit pas contenir un terme qui n'y appartient pas.");
    }

    @Test
    void testRDFAtomIndexOf() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'atome RDF
        RDFAtom rdfAtom = new RDFAtom(subject, predicate, object);

        // Vérification des index
        assertEquals(0, rdfAtom.indexOf(subject), "Le terme 'subject' doit avoir l'index 0.");
        assertEquals(1, rdfAtom.indexOf(predicate), "Le terme 'predicate' doit avoir l'index 1.");
        assertEquals(2, rdfAtom.indexOf(object), "Le terme 'object' doit avoir l'index 2.");
    }

    @Test
    void testRDFAtomConstructorWithList() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création d'une liste de termes
        List<Term> terms = List.of(subject, predicate, object);

        // Création de l'instance
        RDFAtom rdfAtom = new RDFAtom(terms);

        // Assertions
        assertEquals(3, rdfAtom.getTerms().length, "L'atome RDF doit contenir exactement 3 termes.");
        assertEquals("triple", rdfAtom.getPredicate().label(), "Le prédicat doit être 'triple'.");
        assertEquals(3, rdfAtom.getPredicate().arity(), "L'arité du prédicat doit être 3.");
        assertArrayEquals(terms.toArray(), rdfAtom.getTerms(), "Les termes doivent être correctement attribués.");
    }

    @Test
    void testRDFAtomConstructorFromAtom() {
        // Préparation d'un atome
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'atome d'origine
        Atom baseAtom = new AtomImpl(
                SameObjectPredicateFactory.instance().createOrGetPredicate("triple", 3),
                subject, predicate, object);

        // Création de l'instance RDFAtom à partir de l'atom
        RDFAtom rdfAtom = new RDFAtom(baseAtom);

        // Assertions
        assertEquals(3, rdfAtom.getTerms().length, "L'atome RDF doit contenir exactement 3 termes.");
        assertEquals("triple", rdfAtom.getPredicate().label(), "Le prédicat doit être 'triple'.");
        assertEquals(3, rdfAtom.getPredicate().arity(), "L'arité du prédicat doit être 3.");
        assertEquals(subject, rdfAtom.getTripleSubject(), "Le sujet doit correspondre au terme 0.");
        assertEquals(predicate, rdfAtom.getTriplePredicate(), "Le prédicat doit correspondre au terme 1.");
        assertEquals(object, rdfAtom.getTripleObject(), "L'objet doit correspondre au terme 2.");
    }

    @Test
    void testGetTripleSubject() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'atome RDF
        RDFAtom rdfAtom = new RDFAtom(subject, predicate, object);

        // Vérification de la méthode getTripleSubject
        assertEquals(subject, rdfAtom.getTripleSubject(), "La méthode getTripleSubject doit retourner le sujet.");
    }

    @Test
    void testGetTriplePredicate() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'atome RDF
        RDFAtom rdfAtom = new RDFAtom(subject, predicate, object);

        // Vérification de la méthode getTriplePredicate
        assertEquals(predicate, rdfAtom.getTriplePredicate(), "La méthode getTriplePredicate doit retourner le prédicat.");
    }

    @Test
    void testGetTripleObject() {
        // Préparation des termes
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/predicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'atome RDF
        RDFAtom rdfAtom = new RDFAtom(subject, predicate, object);

        // Vérification de la méthode getTripleObject
        assertEquals(object, rdfAtom.getTripleObject(), "La méthode getTripleObject doit retourner l'objet.");
    }

    @Test
    void testRDFAtomConstructorFromInvalidAtom() {
        // Préparation d'un atome avec un prédicat non valide
        Term subject = termFactory.createOrGetLiteral("http://example.org/subject");
        Term predicate = termFactory.createOrGetLiteral("http://example.org/invalidPredicate");
        Term object = termFactory.createOrGetLiteral("http://example.org/object");

        // Création de l'atome d'origine
        Atom baseAtom = new AtomImpl(
                SameObjectPredicateFactory.instance().createOrGetPredicate("notValid", 3),
                subject, predicate, object);

        // Vérification qu'une exception est levée lors de la création d'un RDFAtom
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new RDFAtom(baseAtom); // Devrait lever une exception
        });

        assertTrue(exception.getMessage().contains("Not a triple."), "Le message d'erreur doit indiquer que l'atome n'est pas un triple.");
    }

}
