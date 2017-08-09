package edu.mit.ll.d4m.db.cloud;

import java.util.ArrayList;

/**
 * d4m results set
 *
 * Some data members with getters and setters.
 *
 * @author wi20909
 */
public class D4mDbResultSet {

    /** A list of quads */
    private ArrayList<D4mDbRow> MatlabDbRow = null;
    /** A double for which it is completely the clients responsibility to manage.*/
    private double queryTime = 0;

    public double getQueryTime() {
        return queryTime;
    }

    public void setQueryTime(double queryTime) {
        this.queryTime = queryTime;
    }

    public ArrayList<D4mDbRow> getMatlabDbRow() {
        return MatlabDbRow;
    }

    public void setMatlabDbRow(ArrayList<D4mDbRow> MatlabDbRow) {
        this.MatlabDbRow = MatlabDbRow;
    }
    

}
/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
*/

