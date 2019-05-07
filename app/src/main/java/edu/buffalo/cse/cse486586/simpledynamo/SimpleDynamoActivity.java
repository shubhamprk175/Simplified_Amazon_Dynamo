package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.SharedPreferences;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	static final String TAG = SimpleDynamoActivity.class.getSimpleName();

	static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
	static final int SERVER_PORT = 10000;

	static final String PROVIDER_NAME = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	static final String URL = "content://" + PROVIDER_NAME;


	SharedPreferences prefs;
	public static String val = "file_index";

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
