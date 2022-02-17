package com.helpers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.types.Tuple;

import org.junit.jupiter.api.Test;

public class ParserTupleTest {

    
    @Test
    public void tupleParserWithIdAndDescription() {
        ParserTuple parserTuple = new ParserTuple(5, true, true);

        Tuple result = parserTuple.parse(
            "1 2014 1.6 8 45000 90000 Volkswagen GOL"
        );

        assertEquals(1L, result.getId());
        assertEquals("Volkswagen GOL", result.getDescription().trim());
        assertEquals(5, result.getAttributes().size());
    }

    @Test
    public void tupleParserOnlyAttrs() {
        /* cenario */
        ParserTuple parserTuple = new ParserTuple();
        /* execução */
        Tuple result = parserTuple.parse(
            "2014 1.6 8 45000 90000"
        );
        Double[] arr = result.getAttributes().stream().toArray(Double[]::new);
        /* verificação */
        assertEquals(5, result.getAttributes().size());
        assertArrayEquals(
            new Double[]{2014.0, 1.6, 8.0, 45000.0, 90000.0},
            arr
        );
    }

    @Test
    public void tupleNotInExpectedFormat() {
        ParserTuple parserTuple = new ParserTuple(5, true, true);

        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class, 
            () -> parserTuple.parse("1 2014 1.6 8 45000 90000")
        );

        thrown.getMessage().contains(
            "A string passada como parâmetro não está no formado esperado."
        );
    }

    
}
