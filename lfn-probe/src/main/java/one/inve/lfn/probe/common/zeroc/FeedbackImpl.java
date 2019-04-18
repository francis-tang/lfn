package one.inve.lfn.probe.common.zeroc;

import java.util.Date;

import com.zeroc.Ice.Current;

public class FeedbackImpl implements Feedback {

	@Override
	public String now(Current current) {
		Date date = new Date();
		return date.toString();
	}

}
