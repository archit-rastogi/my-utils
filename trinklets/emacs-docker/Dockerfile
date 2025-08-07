FROM debian:stable-slim
RUN apt-get update && apt-get install -y emacs-nox
# Expose the port Emacs will listen on (e.g., 5000)
EXPOSE 5000
# Start Emacs server on TCP socket
CMD ["emacs", "--daemon", "--server", "--server-use-tcp", "--server-port", "5000"]
