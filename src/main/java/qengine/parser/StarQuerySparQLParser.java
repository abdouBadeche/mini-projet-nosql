package qengine.parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;

import fr.boreal.io.api.Parser;
import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.api.Variable;
import fr.boreal.model.logicalElements.factory.api.TermFactory;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.query.api.Query;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

/**
 * Un parser pour analyser des requêtes SparQL en requêtes en étoile.
 */
public class StarQuerySparQLParser implements Parser<Query> {

    private final Iterator<String> queryIterator;
    private final TermFactory termFactory = SameObjectTermFactory.instance();
    private final SPARQLParser sparqlParser = new SPARQLParser();
    private Query nextQuery = null;

    /**
     * Constructeur.
     *
     * @param sparqlFilePath chemin vers le fichier contenant les requêtes SparQL
     * @throws IOException si le fichier ne peut pas être lu
     */
    public StarQuerySparQLParser(String sparqlFilePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(sparqlFilePath));
        this.queryIterator = lines.iterator();
    }

    @Override
    public boolean hasNext() {
        if (nextQuery != null) {
            return true;
        }

        try {
            while (queryIterator.hasNext()) {
                StringBuilder queryBuilder = new StringBuilder();

                // Construire une requête multi-ligne jusqu'à rencontrer "}"
                while (queryIterator.hasNext()) {
                    String line = queryIterator.next();
                    queryBuilder.append(line).append(System.lineSeparator());

                    if (line.trim().endsWith("}")) {
                        break;
                    }
                }

                String queryString = queryBuilder.toString().trim();
                if (!queryString.isEmpty()) {
                    ParsedQuery parsedQuery = sparqlParser.parseQuery(queryString, null);
                    this.nextQuery = parseStarQuery(parsedQuery);
                    return true;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Erreur lors de l'analyse de la requête SparQL", e);
        }

        return false;
    }

    @Override
    public Query next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Aucune requête disponible");
        }

        Query result = nextQuery;
        nextQuery = null;
        return result;
    }

    /**
     * Parse une requête en étoile à partir d'une requête SparQL analysée.
     *
     * @param parsedQuery la requête SparQL analysée
     * @return une instance de {@link StarQuery}
     * @throws IllegalArgumentException si la requête n'est pas une requête en étoile valide
     */
    private StarQuery parseStarQuery(ParsedQuery parsedQuery) {
        List<StatementPattern> patterns = StatementPatternCollector.process(parsedQuery.getTupleExpr());

        if (patterns.isEmpty()) {
            throw new IllegalArgumentException("La requête SparQL est vide ou ne contient pas de patterns RDF.");
        }

        // Extraire les variables et les triplets RDF
        Map<String, Variable> variables = new HashMap<>();
        List<RDFAtom> rdfAtoms = new ArrayList<>();
        Variable centralVariable = null;

        for (StatementPattern pattern : patterns) {
            Term subject = convertToTerm(pattern.getSubjectVar(), variables);
            Term predicate = convertToTerm(pattern.getPredicateVar(), variables);
            Term object = convertToTerm(pattern.getObjectVar(), variables);

            // Identifier la variable centrale
            if (subject instanceof Variable) {
                centralVariable = updateCentralVariable(centralVariable, (Variable) subject);
            } else if (object instanceof Variable) {
                centralVariable = updateCentralVariable(centralVariable, (Variable) object);
            } else {
                throw new IllegalArgumentException("Aucune variable centrale partagée trouvée dans les triplets RDF.");
            }

            rdfAtoms.add(new RDFAtom(subject, predicate, object));
        }

        if (centralVariable == null) {
            throw new IllegalArgumentException("Impossible de déterminer une variable centrale dans la requête.");
        }

        List<Variable> answerVariables = extractAnswerVariables(parsedQuery, variables);

        // Construire la requête en étoile
        return new StarQuery(parsedQuery.getSourceString(), rdfAtoms, answerVariables);
    }

    /**
     * Met à jour la variable centrale en validant qu'elle est cohérente entre les triplets.
     *
     * @param currentCentral la variable centrale actuelle (peut être null)
     * @param candidate la variable candidate pour devenir centrale
     * @return la variable centrale
     * @throws IllegalArgumentException si plusieurs variables centrales différentes sont détectées
     */
    private Variable updateCentralVariable(Variable currentCentral, Variable candidate) {
        if (currentCentral == null) {
            return candidate;
        } else if (!currentCentral.equals(candidate)) {
            throw new IllegalArgumentException("Plusieurs variables centrales détectées, ce n'est pas une requête en étoile.");
        }
        return currentCentral;
    }

    /**
     * Extrait les variables projetées à partir de la requête SparQL analysée.
     *
     * @param parsedQuery la requête SparQL analysée
     * @param variables le dictionnaire des variables
     * @return la liste des variables projetées
     */
    private List<Variable> extractAnswerVariables(ParsedQuery parsedQuery, Map<String, Variable> variables) {
        ProjectionElemList projectionElemList = getProjectionElemList(parsedQuery);

        return projectionElemList.getElements().stream()
                .map(e -> variables.computeIfAbsent("?" + e.getSourceName(), SameObjectTermFactory.instance()::createOrGetVariable))
                .toList();
    }

    /**
     * Parcourt l'arbre d'expression pour récupérer le nœud `ProjectionElemList`.
     *
     * @param parsedQuery la requête analysée
     * @return la liste des éléments projetés
     * @throws IllegalArgumentException si aucun nœud `ProjectionElemList` n'est trouvé
     */
    private ProjectionElemList getProjectionElemList(ParsedQuery parsedQuery) {
        ProjectionElemList[] projectionElemList = new ProjectionElemList[1]; // Utilisation d'un tableau pour capturer le résultat

        parsedQuery.getTupleExpr().visit(new AbstractQueryModelVisitor<RuntimeException>() {
            @Override
            public void meet(Projection projection) {
                projectionElemList[0] = projection.getProjectionElemList();
            }
        });

        if (projectionElemList[0] == null) {
            throw new IllegalArgumentException("Aucun nœud de projection trouvé dans la requête.");
        }

        return projectionElemList[0];
    }


    /**
     * Convertit une variable de requête RDF4J en une instance de Term.
     *
     * @param var       la variable RDF4J
     * @param variables un cache des variables déjà rencontrées
     * @return une instance de Term correspondante
     */
    private Term convertToTerm(org.eclipse.rdf4j.query.algebra.Var var, Map<String, Variable> variables) {
        if (var.hasValue()) {
            return termFactory.createOrGetLiteral(var.getValue().stringValue());
        }

        return variables.computeIfAbsent("?" + var.getName(), termFactory::createOrGetVariable);
    }

    @Override
    public void close() {
        // Rien à fermer ici
    }
}
