package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.SharedPreferences;

public class PermanentData {

    public PermanentData(SharedPreferences _pref){
        preferences = _pref;
    }

    public void put(int _born){
        preferences.edit().putInt("Born", _born).apply();
    }

    public int get(){
        return preferences.getInt("Born", 0);
    }
    SharedPreferences preferences;
}
