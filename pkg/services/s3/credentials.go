package s3

import "github.com/aws/aws-sdk-go/aws/credentials"

func NewCredentials(accessKey, secretKey string) *credentials.Credentials {
	if accessKey == "" || secretKey == "" {
		return credentials.AnonymousCredentials
	}

	return credentials.NewStaticCredentials(accessKey, secretKey, "")
}
