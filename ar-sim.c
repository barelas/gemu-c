/* simulation of scheduling with Advance Reservations */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* resource states: */
#define AVAILABLE 1
#define HAS_JOBS 2
#define LEAVING 3
#define NO_ACCEPT_JOBS 4

/* job states: */
#define WAITING 1
#define RUNNING 2
#define DONE 3
#define SENDING_DATA 4
#define WAITING_TO_SEND_DATA 5
#define READY_TO_RUN 6

/* interval in seconds between scheduling decisions
 * set to 0 if no interval wished */
#define INTERVAL 0

/* when MAX_JOBS jobs are done, simulation ends */
#define MAX_JOBS 100000

/* every RECORD_INTERVAL jobs, record mean usage of resources */
#define RECORD_INTERVAL 500

/* This defines the probability by which a resource leaves
 * the cluster when it completes a job.
 * The probability is calculated R_PROB/1000.
 * example:if RL_PROB=500, then a resource has 50% chance
 * of leaving the cluster when it completes a job */
#define RL_PROB 300

/* probability to add a resource */
#define ADD_RESOURCE_PROB 50

/* probability to add a job */
#define ADD_JOB_PROB 50

/* function declaration */
void add_remove();
void run_send();
void schedule();
void timeout();
void add_res();
void add_job();
void remove_done_jobs();
void remove_leaving_resources();
void traceall();
void record_mean_usage();

struct resource {
	long int code;
	int state;
	int level;
	float total_time;
	float used_time;
	long int total_workload;
	struct reservation *first_rsv;
	struct reservation *last_rsv;
	struct resource *next;
};

struct job {
	long int code;
	int state;
	int workload;
	int send_data;
	long int wait_time;
	struct job *next;
	struct resource *run_on;
};

struct reservation {
	struct job *job_to_run;
	struct reservation *next_rsv;
};

/* global variables */
struct resource *first_res = NULL; /* always points to the first member of resource list */
struct job *first_job = NULL; /* always points to the first member of job list */
struct resource *last_res = NULL; /* always points to the last member of resource list */
struct job *last_job = NULL; /* always points to the last member of job list */
struct resource *r = NULL; /* general use resource pointer */
struct job *j = NULL; /* general use job pointer */
long int resource_number = 0; /* total number of resources added */
long int job_number = 0; /* total number of jobs submitted */
float mean_usage = 0; /* mean value of resource usage */
float mean_wait_time = 0; /* mean waiting time for jobs to be scheduled */
long int resources_gone = 0; /* number of resources gone */
long int jobs_done = 0; /* number of jobs done */
struct reservation *rsv; /* general use reservation pointer */

int main()
{
	/* go to background */
	if (fork()) exit(0);

	/* set SIGALRM signal handler function */
	if ( signal(SIGALRM, timeout)==SIG_ERR )
		exit(errno);

	for (;;) { /* forever */
		traceall();
		add_remove();
		run_send();
		schedule();
		if (INTERVAL) { /*wait INTERVAL seconds*/
			alarm(INTERVAL);
			pause();
		}
	}
}

void add_remove()
{
	static int begin = 1;
	int i;

	if (begin) {
		for (i=1;i<=5;++i) add_res();
		begin = 0;
	}

	remove_done_jobs();

	remove_leaving_resources();

	i = 1 + (random() % 1000);
	if ( i <= ADD_RESOURCE_PROB )
		add_res();
	else if ( i > ADD_JOB_PROB )
		add_job();
}

void schedule() /*AR scheduling*/
{
	struct job *best_job = NULL;
	struct resource *best_r;

	/* begin with the first waiting job */
	j = first_job;
	while (j) {
		if (j->state==WAITING) {
			best_job = j;
			break;
		}
		j = j->next;
	}

	/* if no waiting job exists, return */
	if (!(j)) return;

	/* if can't malloc() return */
	if ( !(rsv=malloc(sizeof(struct reservation))) ) return;

	/* select best resource */
	best_r = NULL;
	r = first_res;
	while (r) {
		if (r->state != NO_ACCEPT_JOBS) {
			best_r = r;
			break;
		}
		r = r->next;
	}
	while (r) {
		if ( (r->state!=NO_ACCEPT_JOBS)&&(r->total_workload<best_r->total_workload) )
			best_r = r;
		r = r->next;
	}

	/* if no resource exists, return */
	if (!(best_r)) return;

	/* match job with resource */
	best_job->state = WAITING_TO_SEND_DATA;
	best_r->state = HAS_JOBS;
	best_r->total_workload += best_job->workload;
	rsv->job_to_run = best_job;
	rsv->next_rsv = NULL;
	if (best_r->first_rsv) {
		best_r->last_rsv->next_rsv = rsv;
		best_r->last_rsv = rsv;
	} else {
		best_r->first_rsv = rsv;
		best_r->last_rsv = rsv;
	}
}

void timeout()
{
	if ( signal(SIGALRM, timeout)==SIG_ERR )
		exit(errno);
}

void add_res()
{
	if ( !(r = malloc(sizeof(struct resource))) ) return;
	r->code = ++resource_number;
	r->state = AVAILABLE;
	r->level = 1 + (random() % 5);
	r->total_time = 0;
	r->used_time = 0;
	r->total_workload = 0;
	r->first_rsv = NULL;
	r->last_rsv = NULL;
	r->next = NULL;
	if (first_res) {
		last_res->next = r;
		last_res = r;
	} else {
		first_res = r;
		last_res = r;
	}
}

void add_job()
{
	if ( !(j = malloc(sizeof(struct job))) ) return;
	j->code = ++job_number;
	j->state = WAITING;
	j->workload = 50 + (random() % 950);
	j->wait_time = 0;
	j->next = NULL;
	j->run_on = NULL;
	j->send_data = (random() % 30);
	if (first_job) {
		last_job->next = j;
		last_job = j;
	} else {
		first_job = j;
		last_job = j;
	}
}

void remove_done_jobs()
{
	struct job *previous;

	while (j = first_job) {
		if (first_job->state==DONE) {
			first_job = j->next;
			free(j);
		} else break;
	}
	
	while (j) {
		if (j->state==DONE) {
			previous->next = j->next;
			free(j);
		} else {
			previous = j;
		}
		j = previous->next;
	}
}

void remove_leaving_resources()
{
	struct resource *previous;

	while (r = first_res) {
		if (first_res->state==LEAVING) {
			first_res = r->next;
			free(r);
		} else break;
	}
	
	while (r) {
		if (r->state==LEAVING) {
			previous->next = r->next;
			free(r);
		} else {
			previous = r;
		}
		r = previous->next;
	}
}

void run_send()
{
	r = first_res;
	while (r) {
		if ( (r->state==NO_ACCEPT_JOBS)&&(!(r->first_rsv)) ) {
			r->state = LEAVING;
			continue;
		}

		/* run job: */
		if (!(rsv = r->first_rsv)) {
			r = r->next;
			continue;
		}
		switch (rsv->job_to_run->state) {
		case RUNNING:
			rsv->job_to_run->workload -= r->level;
			r->total_workload -= r->level;
			r->used_time++;
			if (rsv->job_to_run->workload < 0) {
				rsv->job_to_run->state = DONE;
				r->first_rsv = rsv->next_rsv;
				free(rsv);
				if ( (random() % 1000) <= RL_PROB ) r->state = NO_ACCEPT_JOBS;
			}
			break;
		case READY_TO_RUN:
			rsv->job_to_run->state = RUNNING;
			break;
		default:
			break;
		}

		/* send input data: */
		rsv = r->first_rsv;
		while (rsv) {
			if (rsv->job_to_run->state == SENDING_DATA) {
				rsv->job_to_run->send_data--;
				if (rsv->job_to_run->send_data <= 0) {
					rsv->job_to_run->state = READY_TO_RUN;
					if (rsv->next_rsv)
						rsv->next_rsv->job_to_run->state = SENDING_DATA;
				}
				break;
			} else if (rsv->job_to_run->state == WAITING_TO_SEND_DATA) {
				rsv->job_to_run->state = SENDING_DATA;
				break;
			}
			rsv = rsv->next_rsv;
		}

		r = r->next;
	}
}

void traceall()
{
	float temp;

	j = first_job;
	while (j) {
		switch (j->state) {
		case WAITING:
		case WAITING_TO_SEND_DATA:
		case READY_TO_RUN:
			j->wait_time++;
			break;
		case DONE:
			jobs_done++;
			/* every RECORD_INTERVAL done jobs, save mean usage and wait time */
			if (!(jobs_done%RECORD_INTERVAL)) record_mean_usage();
			break;
		default:
			break;
		}
		j = j->next;
	}

	r = first_res;
	while (r) {
		r->total_time++;
		switch (r->state) {
		case LEAVING:
			temp = (r->used_time/r->total_time)*100;
			mean_usage = (mean_usage*resources_gone + temp)/(++resources_gone);
			break;
		default:
			break;
		}
		r = r->next;
	}

	/* if MAX_JOBS are complete, exit */
	if (jobs_done >= MAX_JOBS) exit(0);
}

void record_mean_usage()
{
	float temp = 0;
	FILE *fp;
	struct job *j;

	mean_wait_time = 0;
	j = first_job;
	while (j) {
		mean_wait_time = (mean_wait_time*temp + j->wait_time)/(++temp);
		j = j->next;
	}

	if (fp=fopen("ar-sim.out.txt","a")) {
		fprintf(fp,"%i %f %f %i\n",jobs_done,mean_usage,mean_wait_time,job_number);
		fclose(fp);
	}
}