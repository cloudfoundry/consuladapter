package fakes

func NewFakeClient() (*FakeClient, *FakeAgent, *FakeISession) {
	client := &FakeClient{}
	agent := &FakeAgent{}
	session := &FakeISession{}
	client.AgentReturns(agent)
	client.SessionReturns(session)
	return client, agent, session
}
