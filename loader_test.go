package dataloaden_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Warashi/dataloaden"
)

func TestLoader_Load(t *testing.T) {
	fetch := func(keys []int) ([]int, []error) {
		ret := make([]int, len(keys))
		retErr := make([]error, len(keys))
		for i := range keys {
			if keys[i]%2 == 0 {
				ret[i] = keys[i] * 10
			} else {
				retErr[i] = errors.New("some error")
			}
		}
		return ret, retErr
	}
	config := dataloaden.LoaderConfig[int, int]{
		Fetch: fetch,
		Wait:  1 * time.Millisecond,
	}
	loader := dataloaden.NewLoader(config)
	loader.Prime(-1, 1000)

	tests := []struct {
		name    string
		arg     int
		want    int
		wantErr bool
	}{
		{name: "non-error", arg: 0, want: 0, wantErr: false},
		{name: "error", arg: 1, want: 0, wantErr: true},
		{name: "load-from-cache", arg: -1, want: 1000, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loader.Load(tt.arg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Load() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoader_LoadAll(t *testing.T) {
	fetch := func(keys []int) ([]int, []error) {
		ret := make([]int, len(keys))
		retErr := make([]error, len(keys))
		for i := range keys {
			if keys[i]%2 == 0 {
				ret[i] = keys[i] * 10
			} else {
				retErr[i] = errors.New("some error")
			}
		}
		return ret, retErr
	}
	config := dataloaden.LoaderConfig[int, int]{
		Fetch: fetch,
		Wait:  1 * time.Millisecond,
	}
	loader := dataloaden.NewLoader(config)
	loader.Prime(-1, 1000)

	tests := []struct {
		name    string
		arg     []int
		want    []int
		wantErr []bool
	}{
		{name: "normal", arg: []int{-1, 0, 1, 2, 3}, want: []int{1000, 0, 0, 20, 0}, wantErr: []bool{false, false, true, false, true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loader.LoadAll(tt.arg)
			gotErr := make([]bool, len(err))
			for i := range err {
				gotErr[i] = err[i] != nil
			}
			if !reflect.DeepEqual(gotErr, tt.wantErr) {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Load() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoader_Prime(t *testing.T) {
	fetch := func(keys []int) ([]int, []error) {
		ret := make([]int, len(keys))
		retErr := make([]error, len(keys))
		for i := range keys {
			if keys[i]%2 == 0 {
				ret[i] = keys[i] * 10
			} else {
				retErr[i] = errors.New("some error")
			}
		}
		return ret, retErr
	}
	config := dataloaden.LoaderConfig[int, int]{
		Fetch: fetch,
		Wait:  1 * time.Millisecond,
	}
	loader := dataloaden.NewLoader(config)

	if want, got := true, loader.Prime(10, 1000); want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	if want, got := false, loader.Prime(10, 1000); want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}

func TestLoader_Clear(t *testing.T) {
	fetch := func(keys []int) ([]int, []error) {
		ret := make([]int, len(keys))
		retErr := make([]error, len(keys))
		for i := range keys {
			if keys[i]%2 == 0 {
				ret[i] = keys[i] * 10
			} else {
				retErr[i] = errors.New("some error")
			}
		}
		return ret, retErr
	}
	config := dataloaden.LoaderConfig[int, int]{
		Fetch: fetch,
		Wait:  1 * time.Millisecond,
	}
	loader := dataloaden.NewLoader(config)

	if want, got := true, loader.Prime(10, 100); want != got {
		t.Errorf("want %v, got %v", want, got)
	}

	loader.Clear(10)

	if want, got := true, loader.Prime(10, 100); want != got {
		t.Errorf("want %v, got %v", want, got)
	}
}
