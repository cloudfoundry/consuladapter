package fakes

func NewFakeClient() (*FakeClient, *FakeAgent, *FakeKV, *FakeISession) {
	client := &FakeClient{}

	agent := &FakeAgent{}
	kv := &FakeKV{}
	session := &FakeISession{}

	client.AgentReturns(agent)
	client.KVReturns(kv)
	client.SessionReturns(session)
	return client, agent, kv, session
}
