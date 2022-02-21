package com.helpers;

import java.util.StringTokenizer;

import com.types.Point;

/**
 * Realizar o parser de uma string em um objeto do tipo Ponto
 */
public class ParserPoint {
    
    public static Point parse(String str) {
        Point point = new Point();
        StringTokenizer st = new StringTokenizer(str);
        while(st.hasMoreTokens()) {
            point.getAttributes().add(Double.parseDouble(st.nextToken()));
        }
        return point;
    }
}
