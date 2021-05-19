package backoff

import "time"

// ExponentialWait implements exponential backoff and sleeps for calculated duration.
func ExponentialWait(retried, maxBackOff int) {
	delay := min((2^retried)+5, maxBackOff)
	time.Sleep(time.Duration(delay) * time.Second)
}

func min(i, j int) int {
	if i < j {
		return i
	}

	return j
}
