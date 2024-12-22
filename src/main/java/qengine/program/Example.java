package qengine.program;

import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class Example {

	private static final String WORKING_DIR = "data/";
	private static final String SAMPLE_DATA_FILE = WORKING_DIR + "sample_data.nt";
	private static final String SAMPLE_QUERY_FILE = WORKING_DIR + "sample_query.queryset";

	public static void main(String[] args) throws IOException {
		/*
		 * Exemple d'utilisation des deux parsers
		 */
		System.out.println("=== Parsing RDF Data ===");
		List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE);

		System.out.println("\n=== Parsing Sample Queries ===");
		List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE);

		/*
		 * Exemple d'utilisation de l'évaluation de requetes par Integraal avec les objets parsés
		 */
		System.out.println("\n=== Executing the queries with Integraal ===");
		FactBase factBase = new SimpleInMemoryGraphStore();
		for (RDFAtom atom : rdfAtoms) {
			factBase.add(atom);  // Stocker chaque RDFAtom dans le store
		}

		// Exécuter les requêtes sur le store
		for (StarQuery starQuery : starQueries) {
			executeStarQuery(starQuery, factBase);
		}
	}

	/**
	 * Parse et affiche le contenu d'un fichier RDF.
	 *
	 * @param rdfFilePath Chemin vers le fichier RDF à parser
	 * @return Liste des RDFAtoms parsés
	 */
	private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
		FileReader rdfFile = new FileReader(rdfFilePath);
		List<RDFAtom> rdfAtoms = new ArrayList<>();

		try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
			int count = 0;
			while (rdfAtomParser.hasNext()) {
				RDFAtom atom = rdfAtomParser.next();
				rdfAtoms.add(atom);  // Stocker l'atome dans la collection
				System.out.println("RDF Atom #" + (++count) + ": " + atom);
			}
			System.out.println("Total RDF Atoms parsed: " + count);
		}
		return rdfAtoms;
	}

	/**
	 * Parse et affiche le contenu d'un fichier de requêtes SparQL.
	 *
	 * @param queryFilePath Chemin vers le fichier de requêtes SparQL
	 * @return Liste des StarQueries parsées
	 */
	private static List<StarQuery> parseSparQLQueries(String queryFilePath) throws IOException {
		List<StarQuery> starQueries = new ArrayList<>();

		try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(queryFilePath)) {
			int queryCount = 0;

			while (queryParser.hasNext()) {
				Query query = queryParser.next();
				if (query instanceof StarQuery starQuery) {
					starQueries.add(starQuery);  // Stocker la requête dans la collection
					System.out.println("Star Query #" + (++queryCount) + ":");
					System.out.println("  Central Variable: " + starQuery.getCentralVariable().label());
					System.out.println("  RDF Atoms:");
					starQuery.getRdfAtoms().forEach(atom -> System.out.println("    " + atom));
				} else {
					System.err.println("Requête inconnue ignorée.");
				}
			}
			System.out.println("Total Queries parsed: " + starQueries.size());
		}
		return starQueries;
	}

	/**
	 * Exécute une requête en étoile sur le store et affiche les résultats.
	 *
	 * @param starQuery La requête à exécuter
	 * @param factBase  Le store contenant les atomes
	 */
	private static void executeStarQuery(StarQuery starQuery, FactBase factBase) {
		FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery(); // Conversion en FOQuery
		FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance(); // Créer un évaluateur
		Iterator<Substitution> queryResults = evaluator.evaluate(foQuery, factBase); // Évaluer la requête

		System.out.printf("Execution of  %s:%n", starQuery);
		System.out.println("Answers:");
		if (!queryResults.hasNext()) {
			System.out.println("No answer.");
		}
		while (queryResults.hasNext()) {
			Substitution result = queryResults.next();
			System.out.println(result); // Afficher chaque réponse
		}
		System.out.println();
	}
}
