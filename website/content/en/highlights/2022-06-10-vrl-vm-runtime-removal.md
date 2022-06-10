---
date: "2022-06-10"
title: "Removal of VRL VM beta runtime."
description: "Default AST runtime got a speed bump."
authors: ["JeanMertz"]
pr_numbers: [12888]
release: "0.23.0"
hide_on_release_notes: false
badges:
  type: breaking change
---

Back in March, we [announced the beta release of our VM runtime for VRL][post].
In it, we mentioned a performance increase of 10-15% compared to the default
”AST” runtime.

This VM runtime had several downsides for us maintainers, including a more
complex implementation, a highler likelihood of introducing runtime bugs, and
the need to maintain two different runtimes, as we were comitted to keeping the
AST runtime around, for implementation correctness verification purposes.

Despite those downsides for the maintainers, the performance increase was
a clear win for users of VRL to make the maintenance overhead worth the
investment.

However, since March, we’ve identified several performance bottlenecks in our
default AST runtime that allowed us to increase its performance compared to the
VM runtime. These performance improvements were mentioned in the [0.22 release
notes][0.22], and brought the AST runtime on equal footing with the VM runtime
(and additionally increased the performance of _both_ runtimes).

Because of this, we’ve decided **not** to promote the VM runtime out of its beta
state, and instead retire it, keeping the AST as the only available VRL runtime
at this moment.

For those of you using the VM beta runtime, you’ll get an error stating that you
need to remove the following line:

```toml
runtime = "vm"
```

After that, VRL will once again use the AST runtime, with more or less equal
performance characteristics as the VM runtime.

## Going Forward

While this release marks the end of the ”VM” runtime experiment, we are not
closing the door on potential other runtime experiments in the future.

One such experiment we are currently exploring is the possibility of using
[LLVM][] to compile VRL programs to native instructions for an increase in
performance over the existing AST runtime. It’s still too early in the project
to say anything substantial about this experiment, and it might not turn into
something we feel confident in shipping, but keep an eye out for more news from
[@pablosichert][] — who is spearheading this project — on this in the coming
months.

[post]: /highlights/2022-03-15-vrl-vm-beta/
[0.22]: /releases/0.22.0/
[LLVM]: https://llvm.org/
[@pablosichert]: https://github.com/pablosichert
