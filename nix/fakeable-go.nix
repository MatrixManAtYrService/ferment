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
#   fermentDolt              dolt, fakeable, with a SQL parser fix (below)
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

  # goyacc exactly as dolthub/vitess pins it (golang.org/x/tools v0.6.0):
  # regenerating vitess's unpatched grammar with it reproduces the committed
  # parser byte for byte, so the only behavior change is the patch's.
  fermentGoyacc = final.stdenv.mkDerivation {
    pname = "goyacc";
    version = "0.6.0";
    src = final.fetchFromGitHub {
      owner = "golang";
      repo = "tools";
      rev = "v0.6.0";
      hash = "sha256-TpdfTtd+TMAOSunZ2Wj35vDHbqr45aJr1O+PYsAE9Ik=";
    };
    nativeBuildInputs = [ final.go ];
    buildPhase = ''
      export HOME=$TMPDIR GOCACHE=$TMPDIR/go-cache GO111MODULE=off
      go build -o goyacc ./cmd/goyacc
    '';
    installPhase = "install -Dm755 goyacc $out/bin/goyacc";
  };

  # Dolt's parser rejects `ALTER TABLE t ADD COLUMN c ... AFTER reference`
  # (any non-reserved keyword used as a column name after AFTER), which MySQL
  # accepts and real migrations use. The grammar takes `AFTER ID` only;
  # column definitions already allow `all_non_reserved`, so AFTER does too.
  # dolt builds from a local module proxy (proxyVendor), so: unpack the
  # dolthub/vitess module it pins, patch the grammar, regenerate the parser,
  # and point dolt's go.mod at the patched copy (a local replace needs no
  # download).
  fermentDolt = (final.fermentFakeable prev.dolt).overrideAttrs (old: {
    nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ final.fermentGoyacc final.unzip ];
    postConfigure = (old.postConfigure or "") + ''
      zip=$(echo "$goModules"/github.com/dolthub/vitess/@v/*.zip)
      unzip -q "$zip" -d "$TMPDIR/vitess-src"
      vitess=$(echo "$TMPDIR"/vitess-src/github.com/dolthub/vitess@*)
      chmod -R u+w "$vitess"
      p="$vitess/go/vt/sqlparser"
      patch -d "$p" -p1 < ${./dolt-after-nonreserved.patch}
      (cd "$p" && goyacc -o sql.go sql.y && rm -f y.output)
      go mod edit -replace "github.com/dolthub/vitess=$vitess"
    '';
  });
}
