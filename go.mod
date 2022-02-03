module github.com/websbg/go_test_blob

go 1.16

require (
	gocloud.dev v0.24.0
	gopkg.in/ini.v1 v1.66.3 // indirect
)

//replace gocloud.dev => ../go-cloud

replace gocloud.dev => github.com/google/go-cloud v0.24.1-0.20220127195422-22c9230a5b7f
