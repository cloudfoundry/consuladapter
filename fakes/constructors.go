package fakes

func NewFakeClient() (*FakeClient, *FakeAgent, *FakeKV, *FakeSession) {
	client := &FakeClient{}

	agent := &FakeAgent{}
	kv := &FakeKV{}
	session := &FakeSession{}

	client.AgentReturns(agent)
	client.KVReturns(kv)
	client.SessionReturns(session)
	return client, agent, kv, session
}
