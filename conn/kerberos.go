// Package conn @author: Violet-Eva @date  : 2024/12/26 @notes :
package conn

import (
	"os"
	"os/exec"
)

const (
	DefaultKrbConfPath  = "/etc/krb5.conf"
	DefaultKinitProgram = "/usr/bin/kinit"
)

type KrbAuth struct {
	KrbConfPath      string
	KinitProgramPath string
	KeyTabFilePath   string
	Principal        string
}

func NewKrbAuth(krbConfPath, kinitProgramPath, keyTabFilePath, principal string) *KrbAuth {
	if krbConfPath == "" {
		krbConfPath = DefaultKrbConfPath
	}
	if kinitProgramPath == "" {
		kinitProgramPath = DefaultKinitProgram
	}
	return &KrbAuth{
		KrbConfPath:      krbConfPath,
		KinitProgramPath: kinitProgramPath,
		KeyTabFilePath:   keyTabFilePath,
		Principal:        principal,
	}
}

func (ka *KrbAuth) Kinit() error {
	err := os.Setenv("KRB5_CONFIG", ka.KrbConfPath)
	if err != nil {
		return err
	}
	cmd := exec.Command(ka.KinitProgramPath, "-kt", ka.KeyTabFilePath, ka.Principal)
	return cmd.Run()
}
