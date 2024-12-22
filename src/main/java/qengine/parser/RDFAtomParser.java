package qengine.parser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.stream.Stream;

import org.eclipse.rdf4j.rio.RDFFormat;

import fr.boreal.io.api.Parser;
import fr.boreal.io.rdf.RDFParser;
import fr.boreal.io.rdf.RDFTranslationMode;
import fr.boreal.model.logicalElements.api.Atom;
import fr.boreal.model.logicalElements.api.Predicate;
import fr.boreal.model.logicalElements.factory.impl.SameObjectPredicateFactory;
import qengine.model.RDFAtom;

/**
 * Parser pour transformer des triplets RDF en RDFAtom.
 */
public class RDFAtomParser implements Parser<RDFAtom> {

    private static final Predicate TRIPLE_PREDICATE = SameObjectPredicateFactory.instance()
            .createOrGetPredicate("triple", 3);

    private final RDFParser parser;

    public RDFAtomParser(File file) throws IOException {
        this(new FileReader(file), getRDFFormat(file));
    }

    public RDFAtomParser(Reader reader, RDFFormat format) {
        // Utilisation explicite du mode RawRDFTranslator
        this.parser = new RDFParser(reader, format, null, RDFTranslationMode.Raw);
    }

    @Override
    public boolean hasNext() {
        return parser.hasNext();
    }

    @Override
    public RDFAtom next() {
        Object obj = parser.next();

        if (obj instanceof Atom atom) {
            return convertToRDFAtom(atom);
        } else {
            throw new IllegalArgumentException("L'objet parsé n'est pas un atome RDF.");
        }
    }

    @Override
    public void close() {
        parser.close();
    }

    /**
     * Retourne un flux de tous les atomes RDF parsés.
     *
     * @return un flux de RDFAtom
     */
    public Stream<RDFAtom> getRDFAtoms() {
        return this.streamParsedObjects(RDFAtom.class);
    }

    private static RDFFormat getRDFFormat(File file) {
        return org.eclipse.rdf4j.rio.Rio.getParserFormatForFileName(file.getName()).orElse(RDFFormat.TURTLE);
    }

    /**
     * Convertit un atome Integraal standard en RDFAtom.
     *
     * @param atom L'atome à convertir
     * @return L'instance correspondante de RDFAtom
     */
    private RDFAtom convertToRDFAtom(Atom atom) {
        if (atom.getTerms().length != 3) {
            throw new IllegalArgumentException("Un RDFAtom doit contenir exactement trois termes.");
        }

        if (!TRIPLE_PREDICATE.equals(atom.getPredicate())) {
            throw new IllegalArgumentException("Le prédicat de l'atome n'est pas 'triple'.");
        }

        return new RDFAtom(atom.getTerms()[0], atom.getTerms()[1], atom.getTerms()[2]);
    }
}
