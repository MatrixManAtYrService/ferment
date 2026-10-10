# Overlay: make Go programs obey the same fake clock as libfaketime.
#
# libfaketime works by LD_PRELOAD, intercepting libc's time calls. Go doesn't
# read the clock through libc (the runtime goes straight to the vDSO), so Go
# binaries ignore it. A database written in Go (dolt) would then stamp NOW()
# with real time while everything around it is faked.
#
# This patches Go's time.Now() to add the offset from FAKETIME, in the same
# "+N" / "-N" seconds format ferment gives libfaketime, so a Go program built
# with `fermentGo` joins the faked tree with no other changes. Only wall-clock
# time moves; monotonic readings (time.Since, timers) are untouched, matching
# FAKETIME_DONT_FAKE_MONOTONIC. Unset FAKETIME = stock behavior.
#
#   fermentGo                the patched toolchain
#   fermentFakeable pkg      rebuild a buildGoModule package with it
#                            (e.g. fermentFakeable pkgs.dolt)
final: prev: {
  fermentGo = prev.go.overrideAttrs (old: {
    postPatch = (old.postPatch or "") + ''
      substituteInPlace src/time/time.go \
        --replace-fail 'sec, nsec, mono := runtimeNow()' \
                       'sec, nsec, mono := runtimeNow(); sec += fermentFakeOffset()'
      cp ${./ferment_faketime.go} src/time/ferment_faketime.go
    '';
  });

  fermentFakeable = pkg: pkg.override {
    buildGoModule = final.buildGoModule.override { go = final.fermentGo; };
  };
}
