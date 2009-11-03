package com.plexobject.docusearch.lucene.analyzer;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.solr.analysis.SynonymFilter;
import org.apache.solr.analysis.SynonymMap;

import com.plexobject.docusearch.lucene.LuceneUtils;

public class SynonymAnalyzer extends Analyzer {
    private static final String SYNONYMS_DATA = "synonyms.properties";
    final SynonymMap synonymMap;

    public SynonymAnalyzer() {
        this(getDefaultSynonymMap());
    }

    public SynonymAnalyzer(final SynonymMap synonymMap) {
        if (synonymMap == null) {
            throw new NullPointerException("synonymMap is not specified");
        }
        this.synonymMap = synonymMap;
    }

    public TokenStream tokenStream(String fieldName, Reader reader) {
        TokenStream result = new SynonymFilter(new StopFilter(true,
                new LowerCaseFilter(new StandardFilter(new StandardTokenizer(
                        reader))), StandardAnalyzer.STOP_WORDS_SET), synonymMap);
        return result;
    }

    static SynonymMap getDefaultSynonymMap() {
        final boolean ignoreCase = true;
        final SynonymMap synMap = new SynonymMap(ignoreCase);

        final Properties synDefs = new Properties();
        final InputStream in = SynonymAnalyzer.class.getClassLoader()
                .getResourceAsStream(SYNONYMS_DATA);
        if (in != null) {
            try {
                synDefs.load(in);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load " + SYNONYMS_DATA, e);
            }
        }
        for (String name : synDefs.stringPropertyNames()) {
            final String replacement = name.trim();
            final String match = synDefs.getProperty(name).trim();
            final boolean orig = false;
            final boolean merge = true;
            synMap.add(Arrays.asList(match), LuceneUtils.tokens(replacement),
                    orig, merge);
        }
        return synMap;
    }
}
