/**
 * NAME	    :   ANKIT SARRAF
 * EMAIL    :   sarrafan@buffalo.edu
 * PURPOSE  :   Class to handle the LDump Button Click
 * @author sarrafan
 */

package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;

public class OnLDumpClickListener implements OnClickListener {
	private final TextView mTextView;
	private final ContentResolver mContentResolver;
	private final Uri mUri;

	public OnLDumpClickListener(TextView _tv, ContentResolver _cr) {
		mTextView = _tv;
		mContentResolver = _cr;
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public void onClick(View v) {
		Cursor resultCursor = mContentResolver.query(mUri, null, "@", null, null);
		
		while(resultCursor.moveToNext()) {
			String key = resultCursor.getString(0);
			String value = resultCursor.getString(1);
			
			mTextView.append("<" + key + "," + value + ">\n");
		}
		
		mTextView.append("===============\n");
	}
}