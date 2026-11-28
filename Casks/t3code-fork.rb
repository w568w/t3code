cask "t3code-fork" do
  version :latest
  sha256 :no_check

  url "https://github.com/w568w/t3code/releases/download/macos-nightly/T3-Code-Nightly-arm64.dmg"
  name "T3 Code (Fork)"
  desc "Desktop control surface for local coding agents (custom fork)"
  homepage "https://github.com/w568w/t3code"

  depends_on arch: :arm64

  app "T3 Code (Nightly).app"

  postflight do
    system_command "/usr/bin/xattr",
                   args: ["-dr", "com.apple.quarantine", "#{appdir}/T3 Code (Nightly).app"],
                   must_succeed: false
  end

  zap trash: [
    "~/Library/Application Support/T3 Code (Nightly)",
    "~/Library/Preferences/com.t3tools.t3code.plist",
    "~/Library/Saved Application State/com.t3tools.t3code.savedState",
  ]
end
