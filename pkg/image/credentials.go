package image

import (
	"github.com/openshift/library-go/pkg/image/registryclient"
	"github.com/openshift/oc/pkg/cli/image/manifest/dockercredentials"
	"k8s.io/client-go/rest"
)

// NewContext creates a context for the registryClient of `oc mirror`
func NewContext(skipVerification bool) (*registryclient.Context, error) {
	userAgent := rest.DefaultKubernetesUserAgent()
	rt, err := rest.TransportFor(&rest.Config{UserAgent: userAgent})
	if err != nil {
		return nil, err
	}
	insecureRT, err := rest.TransportFor(&rest.Config{TLSClientConfig: rest.TLSClientConfig{Insecure: true}, UserAgent: userAgent})
	if err != nil {
		return nil, err
	}

	ctx := registryclient.NewContext(rt, insecureRT)

	creds, err := dockercredentials.NewCredentialStoreFactory("")
	if err != nil {
		return nil, err
	}
	ctx.WithCredentialsFactory(creds)

	ctx.Retries = 3
	ctx.DisableDigestVerification = skipVerification
	return ctx, nil
}
