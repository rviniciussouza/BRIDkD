package com.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import com.types.Tuple;

/**
 * O parser recebe uma string como entrada e constrói uma Tupla
 * seguindo o formado definido.
 */
public class ParserTuple {
    private Integer dimension;
    private Boolean have_id;
    private Boolean have_desc;
    
    /**
     * Trata todos os valores como atributos
     */
    public ParserTuple() {
        this.dimension = -1;
        this.have_id = false;
        this.have_desc = false;
    }

    /**
     * @param dimension Numero de atributos do registro
     * @param have_id Determina se o primeiro valor é um identificador
     * @param have_desc Determina se os ultimos valores é uma descrição do registro
     */
    public ParserTuple(Integer dimension, Boolean have_id, Boolean have_desc) {
        this.dimension = dimension;
        this.have_id = have_id;
        this.have_desc = have_desc;
    }

    public Tuple parse(String str) throws NullPointerException, NumberFormatException {
        try {
            StringTokenizer st = new StringTokenizer(str);
            Tuple tuple = new Tuple();
            if (this.have_id) {
                tuple.setId(Integer.parseInt(st.nextToken()));
            }
            tuple.setAttributes(this.parseAttributes(st));
            if (this.have_desc) {
                tuple.setDescription(st.nextToken("\n"));
            }
            return tuple;
        }
        catch(NullPointerException nullPointerException) {
            System.err.println("A string passada como parâmetro não está no formado esperado.");
            throw nullPointerException;
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
