package main

import "testing"

func TestFederationRequiresExplicitOptIn(t *testing.T) {
	c, err := parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.publicationPolicy != 0 || c.acceptSeeds {
		t.Fatal("federation enabled without an explicit option")
	}
	c, err = parse([]string{"-federation-publish=external,main", "-federation-accept", "-federation-send-bytes-per-hour=1048576", "-federation-receive-bytes-per-hour=2097152", "-federation-max-blob-bytes=1024"})
	if err != nil {
		t.Fatal(err)
	}
	if c.publicationPolicy == 0 || !c.acceptSeeds || c.sendBytes != 1048576 || c.receiveBytes != 2097152 || c.seedMaxBlob != 1024 {
		t.Fatal("federation options were not applied")
	}
	for _, args := range [][]string{
		{"-federation-publish=off,external"},
		{"-federation-publish=typo"},
		{"-federation-send-bytes-per-hour=0"},
		{"-federation-receive-bytes-per-hour=-1"},
		{"-federation-max-blob-bytes=8589934593"},
	} {
		if _, err := parse(args); err == nil {
			t.Fatalf("accepted invalid policy or limit: %v", args)
		}
	}
}
