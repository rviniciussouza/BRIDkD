package com.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import com.types.Tuple;

/**
 * O parser recebe uma string como entrada e constrói uma Tupla
 * seguindo o formado definido.
 */
public class ParserTuple {
    private int dimension;
    private boolean hasId;
    private boolean hasDesc;
    
    /**
     * Trata todos os valores como atributos
     */
    public ParserTuple() {
        this.dimension = -1;
        this.hasId = false;
        this.hasDesc = false;
    }

    /**
     * @param dimension Numero de atributos do registro
     * @param hasId Determina se o primeiro valor é um identificador
     * @param hasDesc Determina se os ultimos valores é uma descrição do registro
     */
    public ParserTuple(int dimension, boolean hasId, boolean hasDesc) {
        this.dimension = dimension;
        this.hasId = hasId;
        this.hasDesc = hasDesc;
    }

    public Tuple parse(String str) throws IllegalArgumentException, NumberFormatException {
        try {
            StringTokenizer st = new StringTokenizer(str);
            Tuple tuple = new Tuple();
            if (this.hasId) {
                tuple.setId(Integer.parseInt(st.nextToken()));
            }
            tuple.setAttributes(this.parseAttributes(st));
            if (this.hasDesc) {
                tuple.setDescription(st.nextToken("\n"));
            }
            return tuple;
        }
        catch(NoSuchElementException noSuchElementException) {
            System.err.println("A string passada como parâmetro não está no formado esperado.");
            throw new IllegalArgumentException("A string passada como parâmetro não está no formado esperado.");
        }
        catch(NumberFormatException numberFormatException) {
            System.err.println("Alguns valores da string não podem ser tratados.");
            throw numberFormatException;
        }
    }

    private List<Double> parseAttributes(StringTokenizer st) {
        List<Double> attrs = new ArrayList<>();
        if(this.dimension == -1) dimension = st.countTokens();
        for (int i = 0; i < this.dimension; i++) {
            attrs.add(Double.parseDouble(st.nextToken()));
        }
        return attrs;
    }
}
