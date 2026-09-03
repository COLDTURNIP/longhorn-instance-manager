package instance

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"

	"google.golang.org/grpc"

	commonnet "github.com/longhorn/go-common-libs/net"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

type processRequestCaptureServer struct {
	rpc.UnimplementedProcessManagerServiceServer

	mu              sync.Mutex
	createPortArgs  []string
	replacePortArgs []string
}

func (s *processRequestCaptureServer) ProcessCreate(_ context.Context, req *rpc.ProcessCreateRequest) (*rpc.ProcessResponse, error) {
	s.mu.Lock()
	s.createPortArgs = append([]string(nil), req.Spec.PortArgs...)
	s.mu.Unlock()
	return &rpc.ProcessResponse{
		Spec: req.Spec,
		Status: &rpc.ProcessStatus{
			State: types.ProcessStateRunning,
		},
	}, nil
}

func (s *processRequestCaptureServer) ProcessReplace(_ context.Context, req *rpc.ProcessReplaceRequest) (*rpc.ProcessResponse, error) {
	s.mu.Lock()
	s.replacePortArgs = append([]string(nil), req.Spec.PortArgs...)
	s.mu.Unlock()
	return &rpc.ProcessResponse{
		Spec: req.Spec,
		Status: &rpc.ProcessStatus{
			State: types.ProcessStateRunning,
		},
	}, nil
}

func startProcessRequestCaptureServer(t *testing.T) (*processRequestCaptureServer, string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake process manager: %v", err)
	}
	server := grpc.NewServer()
	capture := &processRequestCaptureServer{}
	rpc.RegisterProcessManagerServiceServer(server, capture)
	go func() {
		_ = server.Serve(listener)
	}()

	cleanup := func() {
		server.Stop()
		_ = listener.Close()
	}
	return capture, listener.Addr().String(), cleanup
}

func TestV1InstanceCreateUsesConfiguredIPFamily(t *testing.T) {
	tests := []struct {
		name     string
		ipFamily commonnet.IPFamily
		portArgs []string
	}{
		{name: "unspecified", ipFamily: commonnet.IPFamilyUnspecified, portArgs: []string{"--listen,:"}},
		{name: "ipv4", ipFamily: commonnet.IPFamilyIPv4, portArgs: []string{"--listen,0.0.0.0:"}},
		{name: "ipv6", ipFamily: commonnet.IPFamilyIPv6, portArgs: []string{"--listen,[::]:"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capture, address, cleanup := startProcessRequestCaptureServer(t)
			defer cleanup()

			ops := V1DataEngineInstanceOps{
				processManagerServiceAddress: address,
				ipFamily:                     test.ipFamily,
			}
			request := &rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{
				Name:       "engine-" + test.name,
				Type:       types.InstanceTypeEngine,
				DataEngine: rpc.DataEngine_DATA_ENGINE_V1,
				PortCount:  1,
				PortArgs:   []string{"--listen,:"},
				ProcessInstanceSpec: &rpc.ProcessInstanceSpec{
					Binary: "longhorn-engine",
				},
			}}

			if _, err := ops.InstanceCreate(request); err != nil {
				t.Fatalf("InstanceCreate returned an error: %v", err)
			}

			capture.mu.Lock()
			gotPortArgs := append([]string(nil), capture.createPortArgs...)
			capture.mu.Unlock()
			if !reflect.DeepEqual(gotPortArgs, test.portArgs) {
				t.Fatalf("ProcessCreate port args = %v, want %v", gotPortArgs, test.portArgs)
			}
		})
	}
}

func TestV1InstanceReplaceUsesConfiguredIPFamily(t *testing.T) {
	capture, address, cleanup := startProcessRequestCaptureServer(t)
	defer cleanup()

	ops := V1DataEngineInstanceOps{
		processManagerServiceAddress: address,
		ipFamily:                     commonnet.IPFamilyIPv6,
	}
	request := &rpc.InstanceReplaceRequest{Spec: &rpc.InstanceSpec{
		Name:       "engine",
		Type:       types.InstanceTypeEngine,
		DataEngine: rpc.DataEngine_DATA_ENGINE_V1,
		PortCount:  1,
		PortArgs:   []string{"--listen,:"},
		ProcessInstanceSpec: &rpc.ProcessInstanceSpec{
			Binary: "longhorn-engine",
		},
	}, TerminateSignal: "SIGHUP"}

	if _, err := ops.InstanceReplace(request); err != nil {
		t.Fatalf("InstanceReplace returned an error: %v", err)
	}

	capture.mu.Lock()
	gotPortArgs := append([]string(nil), capture.replacePortArgs...)
	capture.mu.Unlock()
	wantPortArgs := []string{"--listen,[::]:"}
	if !reflect.DeepEqual(gotPortArgs, wantPortArgs) {
		t.Fatalf("ProcessReplace port args = %v, want %v", gotPortArgs, wantPortArgs)
	}
}
