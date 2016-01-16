package consuladapter

import "github.com/hashicorp/consul/api"

//go:generate counterfeiter -o fakes/fake_client.go . Client

type Client interface {
	Agent() Agent
	Session() ISession
}

//go:generate counterfeiter -o fakes/fake_agent.go . Agent

type Agent interface {
	Checks() (map[string]*api.AgentCheck, error)
	Services() (map[string]*api.AgentService, error)
	ServiceRegister(service *api.AgentServiceRegistration) error
	ServiceDeregister(serviceID string) error
	PassTTL(checkID, note string) error
	WarnTTL(checkID, note string) error
	FailTTL(checkID, note string) error
}

//go:generate counterfeiter -o fakes/fake_isession.go . ISession

// We need to rename this to Session once we fix the existing Session type in
// this package
type ISession interface {
	Create(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error)
	CreateNoChecks(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error)
	Destroy(id string, q *api.WriteOptions) (*api.WriteMeta, error)
	Info(id string, q *api.QueryOptions) (*api.SessionEntry, *api.QueryMeta, error)
	List(q *api.QueryOptions) ([]*api.SessionEntry, *api.QueryMeta, error)
	Node(node string, q *api.QueryOptions) ([]*api.SessionEntry, *api.QueryMeta, error)
	Renew(id string, q *api.WriteOptions) (*api.SessionEntry, *api.WriteMeta, error)
	RenewPeriodic(initialTTL string, id string, q *api.WriteOptions, doneCh chan struct{}) error
}

type client struct {
	client *api.Client
}

func NewConsulClient(c *api.Client) Client {
	return &client{client: c}
}

func (c *client) Agent() Agent {
	return NewConsulAgent(c.client.Agent())
}

func (c *client) Session() ISession {
	return NewConsulSession(c.client.Session())
}

type session struct {
	session *api.Session
}

func NewConsulSession(s *api.Session) ISession {
	return &session{session: s}
}

func (s *session) Create(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error) {
	return s.session.Create(se, q)
}

func (s *session) CreateNoChecks(se *api.SessionEntry, q *api.WriteOptions) (string, *api.WriteMeta, error) {
	return s.session.CreateNoChecks(se, q)
}

func (s *session) Destroy(id string, q *api.WriteOptions) (*api.WriteMeta, error) {
	return s.session.Destroy(id, q)
}

func (s *session) Info(id string, q *api.QueryOptions) (*api.SessionEntry, *api.QueryMeta, error) {
	return s.session.Info(id, q)
}

func (s *session) List(q *api.QueryOptions) ([]*api.SessionEntry, *api.QueryMeta, error) {
	return s.session.List(q)
}

func (s *session) Node(node string, q *api.QueryOptions) ([]*api.SessionEntry, *api.QueryMeta, error) {
	return s.session.Node(node, q)
}

func (s *session) Renew(id string, q *api.WriteOptions) (*api.SessionEntry, *api.WriteMeta, error) {
	return s.session.Renew(id, q)
}

func (s *session) RenewPeriodic(initialTTL string, id string, q *api.WriteOptions, doneCh chan struct{}) error {
	return s.session.RenewPeriodic(initialTTL, id, q, doneCh)
}

type agent struct {
	agent *api.Agent
}

func NewConsulAgent(a *api.Agent) Agent {
	return &agent{agent: a}
}

func (a *agent) Checks() (map[string]*api.AgentCheck, error) {
	return a.agent.Checks()
}

func (a *agent) Services() (map[string]*api.AgentService, error) {
	return a.agent.Services()
}

func (a *agent) ServiceRegister(service *api.AgentServiceRegistration) error {
	return a.agent.ServiceRegister(service)
}

func (a *agent) ServiceDeregister(serviceID string) error {
	return a.agent.ServiceDeregister(serviceID)
}

func (a *agent) PassTTL(checkID, note string) error {
	return a.agent.PassTTL(checkID, note)
}

func (a *agent) WarnTTL(checkID, note string) error {
	return a.agent.WarnTTL(checkID, note)
}

func (a *agent) FailTTL(checkID, note string) error {
	return a.agent.FailTTL(checkID, note)
}
